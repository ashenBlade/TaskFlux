using System.Buffers.Binary;
using FluentAssertions;
using Moq;
using Serilog.Core;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Persistence.Metadata;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Consensus.Tests.Infrastructure;
using TaskFlux.Consensus.Tests.Stubs;
using TaskFlux.Core;
using IApplication = TaskFlux.Consensus.Tests.Infrastructure.IApplication;
using TaskCompletionSource = System.Threading.Tasks.TaskCompletionSource;

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Raft")]
public class LeaderStateTests
{
    private static readonly NodeId NodeId = new(1);
    private static readonly NodeId AnotherNodeId = new(NodeId.Id + 1);

    private class IntDeltaExtractor : IDeltaExtractor<int>
    {
        public bool TryGetDelta(int response, out byte[] delta)
        {
            delta = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(delta, response);
            return true;
        }
    }

    private static readonly IDeltaExtractor<int> DeltaExtractor = new IntDeltaExtractor();

    private RaftConsensusModule CreateLeaderNode(Term term,
                                                 NodeId? votedFor,
                                                 ITimer? heartbeatTimer = null,
                                                 IEnumerable<IPeer>? peers = null,
                                                 LogEntry[]? logEntries = null,
                                                 IApplication? application = null,
                                                 IBackgroundJobQueue? jobQueue = null)
    {
        var peerGroup = new PeerGroup(peers switch
                                      {
                                          null      => Array.Empty<IPeer>(),
                                          IPeer[] p => p,
                                          not null  => peers.ToArray(),
                                      });
        var timerFactory = heartbeatTimer is null
                               ? Helpers.NullTimerFactory
                               : new ConstantTimerFactory(heartbeatTimer);
        var facade = CreateFacade();
        if (logEntries is not null)
        {
            facade.SetupTest(logEntries);
        }

        var factory = new Mock<IApplicationFactory<int, int>>().Apply(m =>
        {
            m.Setup(x => x.Restore(It.IsAny<ISnapshot?>(), It.IsAny<IEnumerable<byte[]>>()))
             .Returns(application ?? Helpers.NullApplication);
        });

        jobQueue ??= Helpers.NullBackgroundJobQueue;

        var node = new RaftConsensusModule(NodeId, peerGroup, Logger.None, timerFactory,
            jobQueue, facade, DeltaExtractor, factory.Object);
        node.SetStateTest(node.CreateLeaderState());
        return node;

        FileSystemPersistenceFacade CreateFacade()
        {
            var fs = Helpers.CreateFileSystem();
            var log = FileLog.Initialize(fs.DataDirectory);
            var metadata = MetadataFile.Initialize(fs.DataDirectory);
            metadata.SetupMetadataTest(term, votedFor);
            var snapshotStorage = SnapshotFile.Initialize(fs.DataDirectory);
            return new FileSystemPersistenceFacade(log, metadata, snapshotStorage, Logger.None);
        }
    }

    [Fact]
    public void RequestVote__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var term = new Term(1);
        var expectedTerm = term.Increment();
        using var node = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(AnotherNodeId, expectedTerm, LogEntryInfo.Tomb);
        var response = node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.VoteGranted);
        Assert.Equal(expectedTerm, response.CurrentTerm);
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(3, 2)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void RequestVote__СТермомМеньшеСвоего__ДолженОтветитьОтрицательно(
        int myTerm,
        int otherTerm)
    {
        var term = new Term(myTerm);

        using var node = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(CandidateId: AnotherNodeId,
            CandidateTerm: new(otherTerm),
            LastLogEntryInfo: node.Persistence.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеНоЛогОтстает__НеДолженОтдатьГолос()
    {
        var term = new Term(1);
        var existingLog = new[]
        {
            IntLogEntry(term), // 0
            IntLogEntry(term), // 1
            IntLogEntry(term), // 2
            IntLogEntry(term), // 3
        };

        using var node = CreateLeaderNode(term, null, logEntries: existingLog);
        var greaterTerm = term.Increment();

        // Конфликт на 2 индексе - термы расходятся
        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(term, 2));
        var response = node.Handle(request);

        response.VoteGranted
                .Should()
                .BeFalse("Лог конфликтует - голос не может быть отдан");
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеНоЛогОтстает__ДолженОбновитьТерм()
    {
        var term = new Term(1);
        var existingLog = new[]
        {
            IntLogEntry(term), // 0
            IntLogEntry(term), // 1
            IntLogEntry(term), // 2
            IntLogEntry(term), // 3
        };

        using var node = CreateLeaderNode(term, null, logEntries: existingLog);
        var greaterTerm = term.Increment();

        // Конфликт на 2 индексе - термы расходятся
        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(term, 2));
        _ = node.Handle(request);

        node.CurrentTerm
            .Should()
            .Be(greaterTerm, "терм в запросе был больше");
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеНоЛогОтстает__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var existingLog = new[]
        {
            IntLogEntry(term), // 0
            IntLogEntry(term), // 1
            IntLogEntry(term), // 2
            IntLogEntry(term), // 3
        };

        using var node = CreateLeaderNode(term, null, logEntries: existingLog);
        var greaterTerm = term.Increment();

        // Конфликт на 2 индексе - термы расходятся
        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(term, 2));
        _ = node.Handle(request);

        node.CurrentRole
            .Should()
            .Be(NodeRole.Follower, "При получении большего терма нужно стать последователем");
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогБолееАктуальный__ДолженСтатьПоследователем()
    {
        var term = new Term(1);
        var existingLog = new[]
        {
            IntLogEntry(term), // 0
            IntLogEntry(term), // 1
            IntLogEntry(term), // 2
            IntLogEntry(term), // 3
        };

        using var node = CreateLeaderNode(term, null, logEntries: existingLog);
        var greaterTerm = term.Increment();

        // Добавлена 1 новая запись - после всех моих записей в конце лога с новым термом
        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(greaterTerm, 4));
        _ = node.Handle(request);

        node.CurrentRole
            .Should()
            .Be(NodeRole.Follower, "При получении большего терма нужно стать последователем");
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогБолееАктуальный__ДолженОтдатьГолос()
    {
        var term = new Term(1);
        var existingLog = new[]
        {
            IntLogEntry(term), // 0
            IntLogEntry(term), // 1
            IntLogEntry(term), // 2
            IntLogEntry(term), // 3
        };

        using var node = CreateLeaderNode(term, null, logEntries: existingLog);
        var greaterTerm = term.Increment();

        // Добавлена 1 новая запись - после всех моих записей в конце лога с новым термом
        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(greaterTerm, 4));
        var response = node.Handle(request);

        response.VoteGranted
                .Should()
                .BeTrue("лог не кофликтует и терм больше");
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогБолееАктуальный__ДолженОбновитьТерм()
    {
        var term = new Term(1);
        var existingLog = new[]
        {
            IntLogEntry(term), // 0
            IntLogEntry(term), // 1
            IntLogEntry(term), // 2
            IntLogEntry(term), // 3
        };

        using var node = CreateLeaderNode(term, null, logEntries: existingLog);
        var greaterTerm = term.Increment();

        // Добавлена 1 новая запись - после всех моих записей в конце лога с новым термом
        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(greaterTerm, 4));
        var response = node.Handle(request);

        response.CurrentTerm
                .Should()
                .Be(greaterTerm, "терм в ответе должен быть уже обновленным");
        node.CurrentTerm
            .Should()
            .Be(greaterTerm, "терм в запросе больше текущего");
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    [InlineData(3, 1)]
    [InlineData(3, 2)]
    [InlineData(100, 2)]
    [InlineData(100, 99)]
    [InlineData(1001, 1000)]
    public void AppendEntries__СТермомМеньшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var term = new Term(myTerm);

        using var node = CreateLeaderNode(term, null);

        var request = AppendEntriesRequest.Heartbeat(new(otherTerm), node.Persistence.CommitIndex, AnotherNodeId,
            node.Persistence.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаПереданныйТермБольше__ДолженСтатьFollower()
    {
        var term = new Term(1);
        using var node = CreateLeaderNode(term, null);
        var expectedTerm = term.Increment();
        var request = AppendEntriesRequest.Heartbeat(expectedTerm, node.Persistence.CommitIndex,
            AnotherNodeId, node.Persistence.LastEntry);

        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, response.Term);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Fact]
    public void AppendEntries__КогдаУзелОтветилОтрицательноИЕгоТермБольше__ДолженПерейтиВFollower()
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Schedule());
        });

        var peerTerm = term.Increment();

        var peer = CreateDefaultPeer();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(peerTerm, false));
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object,
            peers: new[] {peer.Object},
            jobQueue: queue);

        heartbeatTimer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void AppendEntries__КогдаЛогУзлаБылПуст__ДолженСинхронизироватьЛогПриСтарте()
    {
        var term = new Term(4);
        var heartbeatTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Schedule())
             .Verifiable();
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        // В логе изначально было 4 записи
        var committed = new[] {IntLogEntry(1), IntLogEntry(1), IntLogEntry(2), IntLogEntry(3),};

        var peer = CreateDefaultPeer();

        // Достигнуто ли начало лога
        var beginReached = false;
        var sentEntries = Array.Empty<LogEntry>();
        peer.Setup(x =>
                 x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns((AppendEntriesRequest request, CancellationToken _) =>
             {
                 // Откатываемся до момента начала лога (полностью пуст)
                 if (request.PrevLogEntryInfo.IsTomb)
                 {
                     if (beginReached)
                     {
                         throw new InvalidOperationException("Уже был отправлен запрос с логом с самого начала");
                     }

                     beginReached = true;
                     sentEntries = request.Entries.ToArray();
                     return AppendEntriesResponse.Ok(request.Term);
                 }

                 return AppendEntriesResponse.Fail(request.Term);
             });
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object, peers: new[] {peer.Object},
            jobQueue: queue);

        // Выставляем изначальный лог в 4 команды
        node.Persistence.Log.SetupLogTest(committed);

        heartbeatTimer.Raise(x => x.Timeout += null);

        beginReached.Should()
                    .BeTrue("Окончание репликации должно быть закончено, когда достигнуто начало лога");
        sentEntries.Should()
                   .BeEquivalentTo(committed, options => options.Using(Comparer),
                        "Отправленные записи должны полностью соответствовать логу");
    }

    [Fact]
    public void AppendEntries__НаСтартеПриложенияКогдаУзел1ИБылНеПолностьюПуст__ДолженСинхронизироватьЛог()
    {
        var term = new Term(4);
        var heartbeatTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Schedule())
             .Verifiable();
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        // В логе изначально было 4 записи
        var existingLogEntries = new[]
        {
            IntLogEntry(1), // 0
            IntLogEntry(1), // 1
            IntLogEntry(2), // 2
            IntLogEntry(3), // 3
        };
        // В логе узла есть все записи до 2 (индекс = 1)
        var storedEntriesIndex = 1;

        var expectedSent = existingLogEntries
                          .Skip(storedEntriesIndex + 1)
                          .ToArray();

        var peer = CreateDefaultPeer();

        // Достигнуто ли начало лога
        var beginReached = false;
        // Отправленные записи при достижении
        var sentEntries = Array.Empty<LogEntry>();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns((AppendEntriesRequest request, CancellationToken _) =>
             {
                 // Откатываемся до момента начала лога (полностью пуст)
                 if (request.PrevLogEntryInfo.Index == storedEntriesIndex)
                 {
                     if (beginReached)
                     {
                         throw new InvalidOperationException("Уже был отправлен запрос с требуемым логом");
                     }

                     beginReached = true;
                     sentEntries = request.Entries.ToArray();
                     return AppendEntriesResponse.Ok(request.Term);
                 }

                 return AppendEntriesResponse.Fail(request.Term);
             });
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object,
            peers: new[] {peer.Object},
            logEntries: existingLogEntries,
            jobQueue: queue);

        heartbeatTimer.Raise(x => x.Timeout += null);

        beginReached.Should()
                    .BeTrue("Окончание репликации должно быть закончено, когда достигнуто начало лога");
        sentEntries.Should()
                   .BeEquivalentTo(expectedSent, options => options.Using(Comparer),
                        "Отправленные записи должны полностью соответствовать логу");
    }

    [Fact]
    public void ПриСтарте__КогдаЕстьДругойУзелКластераИОнПуст__ДолженСинхронизироватьЛогУзла()
    {
        var term = new Term(4);
        var heartbeatTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Schedule())
             .Verifiable();
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        // В логе изначально было 4 записи
        var existingLogEntries = new[]
        {
            IntLogEntry(1), // 0
            IntLogEntry(1), // 1
            IntLogEntry(2), // 2
            IntLogEntry(3), // 3
        };
        // В логе узла есть все записи до 2 (индекс = 1)
        var storedEntriesIndex = -1;

        var expectedSent = existingLogEntries;

        var peer = CreateDefaultPeer();

        // Достигнуто ли начало лога
        var beginReached = false;
        // Отправленные записи при достижении
        var sentEntries = Array.Empty<LogEntry>();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns((AppendEntriesRequest request, CancellationToken _) =>
             {
                 // Откатываемся до момента начала лога (полностью пуст)
                 if (request.PrevLogEntryInfo.Index == storedEntriesIndex)
                 {
                     if (beginReached)
                     {
                         throw new InvalidOperationException("Уже был отправлен запрос с требуемым логом");
                     }

                     beginReached = true;
                     sentEntries = request.Entries.ToArray();
                     return AppendEntriesResponse.Ok(request.Term);
                 }

                 // Откатываемся до момента, пока не получим -1 (самое начало лога)
                 return AppendEntriesResponse.Fail(request.Term);
             });
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object,
            peers: new[] {peer.Object},
            logEntries: existingLogEntries,
            jobQueue: queue);

        heartbeatTimer.Raise(x => x.Timeout += null);

        beginReached.Should()
                    .BeTrue("Окончание репликации должно быть закончено, когда достигнуто начало лога");
        sentEntries.Should()
                   .BeEquivalentTo(expectedSent, options => options.Using(Comparer),
                        "Отправленные записи должны полностью соответствовать логу");
    }

    [Fact(Timeout = 1000 * 5)]
    public async Task
        ПриСтарте__КогдаЕстьКластерИз3УзловИЧастьУзловНеДоКонцаРеплицирована__ДолженСинхронизироватьЛогиУзлов()
    {
        var term = new Term(4);
        var heartbeatTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Schedule())
             .Verifiable();
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        // В логе изначально было 4 записи
        var existingLogEntries = new[]
        {
            IntLogEntry(1), // 0
            IntLogEntry(1), // 1
            IntLogEntry(2), // 2
            IntLogEntry(3), // 3
            IntLogEntry(4), // 4
            IntLogEntry(4), // 5
        };

        // Лог первого узла полностью пуст
        var (first, firstTcs) = CreateStubPeer(-1, existingLogEntries);
        // Второй узел реплицирован до 2 индекса включительно
        var (second, secondTcs) = CreateStubPeer(2, existingLogEntries.Skip(3));

        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object,
            peers: new[] {first.Object, second.Object},
            logEntries: existingLogEntries,
            jobQueue: queue);

        heartbeatTimer.Raise(x => x.Timeout += null);

        await Task.WhenAll(firstTcs.Task, secondTcs.Task);

        return;

        static (Mock<IPeer> Peer, TaskCompletionSource ReplicationEndTcs) CreateStubPeer(
            int logStartIndex,
            IEnumerable<LogEntry> expectedEntries)
        {
            var completed = false;
            var peer = CreateDefaultPeer();
            var nodeCompletionTcs = new TaskCompletionSource();
            peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
                .Returns((AppendEntriesRequest request, CancellationToken _) =>
                 {
                     // Откатываемся до момента начала лога (полностью пуст)
                     if (request.PrevLogEntryInfo.Index == logStartIndex)
                     {
                         if (completed)
                         {
                             throw new InvalidOperationException("Уже был отправлен запрос с требуемым логом");
                         }

                         completed = true;
                         var sentEntries = request.Entries.ToArray();
                         if (!sentEntries.SequenceEqual(expectedEntries, Comparer))
                         {
                             nodeCompletionTcs.TrySetException(
                                 new InvalidDataException("Отправленные записи не равны ожидаемым"));
                         }
                         else
                         {
                             nodeCompletionTcs.TrySetResult();
                         }

                         return AppendEntriesResponse.Ok(request.Term);
                     }

                     // Откатываемся до момента, пока не получим -1 (самое начало лога)
                     return AppendEntriesResponse.Fail(request.Term);
                 });
            return ( peer, nodeCompletionTcs );
        }
    }

    private static LogEntry IntLogEntry(Term term)
    {
        var data = new byte[Random.Shared.Next(0, 128)];
        Random.Shared.NextBytes(data);
        return new LogEntry(term, data);
    }

    [Fact]
    public void AppendEntries__КогдаЛогБылПустИТермБольше__ДолженДобавитьЗаписиВЛог()
    {
        var term = new Term(3);
        using var node = CreateLeaderNode(term, null);
        var expectedTerm = term.Increment();
        var requestEntries = new[] {IntLogEntry(1), IntLogEntry(2), IntLogEntry(2), IntLogEntry(3),};
        var request =
            new AppendEntriesRequest(expectedTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb, requestEntries);
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, node.CurrentTerm);
        Assert.Equal(requestEntries, node.Persistence.Log.ReadAllTest(), Comparer);
    }

    private static readonly LogEntryEqualityComparer Comparer = LogEntryEqualityComparer.Instance;

    [Theory]
    [InlineData(0, 1)]
    [InlineData(0, 2)]
    [InlineData(0, 5)]
    [InlineData(1, 5)]
    [InlineData(1, 10000)]
    [InlineData(-1, 0)]
    [InlineData(-1, 123)]
    public void AppendEntries__КогдаИндексКоммитаБольше__ДолженОбновитьИндексКоммита(
        int oldCommitIndex,
        int newCommitIndex)
    {
        var term = new Term(3);
        var expectedTerm = term.Increment();
        var logEntries = Enumerable.Range(1, newCommitIndex + 1) // Размер лога ровно сколько нужно будет закоммитить
                                   .Select(i => IntLogEntry(i))
                                   .ToArray();
        using var node = CreateLeaderNode(term, null);
        node.Persistence.SetupTest(logEntries: logEntries,
            commitIndex: oldCommitIndex);

        var request = new AppendEntriesRequest(expectedTerm, newCommitIndex, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>());
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, response.Term);
        Assert.Equal(newCommitIndex, node.Persistence.CommitIndex);
    }

    [Fact]
    public void SubmitRequest__КогдаДругихУзловНет__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var mock = new Mock<IApplication>();
        var command = 1;
        var expectedResponse = 123;
        var request = command;
        mock.Setup(x => x.Apply(It.Is<int>(y => y == command)))
            .Returns(expectedResponse)
            .Verifiable();
        using var node = CreateLeaderNode(term, null, peers: Array.Empty<IPeer>(), application: mock.Object);

        var response = node.Handle(request);

        Assert.Equal(expectedResponse, response.Response);
        var committedEntry = node.Persistence.Log.GetAllEntriesTest().Single();
        AssertCommandEqual(committedEntry, expectedResponse);
        mock.Verify(x => x.Apply(It.Is<int>(y => y == command)), Times.Once());
    }

    private static void AssertCommandEqual(LogEntry entry, int expected)
    {
        var actual = BinaryPrimitives.ReadInt32BigEndian(entry.Data);
        Assert.Equal(expected, actual);
    }

    [Fact(Timeout = 1000)]
    public void SubmitRequest__КогдаЕдинственныйДругойУзелОтветилУспехом__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var machine = new Mock<IApplication>();
        var command = 1;
        var expectedResponse = 123;
        var peer = CreateDefaultPeer();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(term, true))
            .Verifiable();
        machine.Setup(x => x.Apply(It.Is<int>(y => y == command)))
               .Returns(expectedResponse)
               .Verifiable();
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            peers: new[] {peer.Object},
            application: machine.Object,
            jobQueue: queue);

        var response = node.Handle(command);

        Assert.Equal(expectedResponse, response.Response);
        var committedEntry = node.Persistence.Log.GetAllEntriesTest().Single();
        AssertCommandEqual(committedEntry, expectedResponse);
        machine.Verify(x => x.Apply(It.Is<int>(y => y == command)), Times.Once());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(10)]
    public void SubmitRequest__ПослеУспешнойРепликации__ДолженВыставитьИндексКоммитаВИндексДобавленнойЗаписи(
        int peersCount)
    {
        var term = new Term(1);
        var peers = Enumerable.Range(0, peersCount)
                              .Select((_, _) => CreateDefaultPeer()
                                  .Apply(m =>
                                   {
                                       m.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                                             It.IsAny<CancellationToken>()))
                                        .Returns(AppendEntriesResponse.Ok(term));
                                   }))
                              .ToArray(peersCount);
        var alreadyCommittedEntries = new LogEntry[]
        {
            new(2, "hello, world"u8.ToArray()), // 0
            new(2, "asdfasdf"u8.ToArray()),     // 1
            new(2, "324234"u8.ToArray()),       // 2
            new(2, "qwtqebrqe"u8.ToArray()),    // 3
        };
        var expectedCommittedIndex = 4; // Добавленная запись будет иметь следующий индекс - 3 + 1 = 4
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            peers: peers.Select(x => x.Object),
            jobQueue: queue,
            logEntries: alreadyCommittedEntries);

        var request = 123;
        _ = node.Handle(request);

        var actual = node.Persistence.CommitIndex;
        Assert.Equal(expectedCommittedIndex, actual);
    }

    private static Mock<IPeer> CreateDefaultPeer()
    {
        return new Mock<IPeer>(MockBehavior.Strict).Apply(m =>
        {
            m.SetupGet(x => x.Id).Returns(AnotherNodeId);
        });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void SubmitRequest__КогдаКластерОтветилБольшинством__ДолженПрименитьКоманду(int peersCount)
    {
        var term = new Term(1);
        var peers = Enumerable.Range(0, peersCount)
                              .Select((_, _) => CreateDefaultPeer()
                                  .Apply(m =>
                                   {
                                       m.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                                             It.IsAny<CancellationToken>()))
                                        .Returns(AppendEntriesResponse.Ok(term));
                                   }))
                              .ToArray(peersCount);
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null,
            peers: peers.Select(x => x.Object),
            jobQueue: queue);

        var request = 123;
        var response = node.Handle(request);

        response.WasLeader
                .Should()
                .BeTrue("Узел был лидером. Другие узлы ничего не посылали");
    }

    [Fact]
    public void SubmitRequest__КогдаУзелОтветилБольшимТермомВоВремяРепликации__ДолженВернутьНеЛидер()
    {
        var term = new Term(1);
        var peer = CreateDefaultPeer();
        var greaterTerm = term.Increment();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new AppendEntriesResponse(greaterTerm, false));
        using var queue = new TaskBackgroundJobQueue();
        using var node = CreateLeaderNode(term, null, peers: new[] {peer.Object}, jobQueue: queue);
        var request = 123;
        var response = node.Handle(request);

        response.WasLeader
                .Should()
                .BeFalse("Если во время репликации узел ответил большим термом - то текущий узел уже не лидер");
        response.HasValue
                .Should()
                .BeFalse("если узел не был лидером, то объекта ответа не может быть");
        node.CurrentTerm
            .Should()
            .Be(greaterTerm, "нужно обновлять терм, когда другой узел ответил большим");
        node.CurrentRole
            .Should()
            .Be(NodeRole.Follower, "нужно становиться последователем, когда другой узел ответил большим термом");
    }

    private class TaskBackgroundJobQueue : IBackgroundJobQueue, IDisposable
    {
        private readonly List<Task> _tasks = new();

        public void Accept(IBackgroundJob job, CancellationToken token)
        {
            _tasks.Add(Task.Run(() => job.Run(token), token));
        }

        public void Dispose()
        {
            try
            {
                Task.WaitAll(_tasks.ToArray());
            }
            catch (AggregateException ae) when (ae.InnerException is OperationCanceledException)
            {
                /*
                 * Фоновые потоки обработчики могли не окончить обработку запроса
                 * прежде чем большинство проголосует за (вернет успешный ответ)
                 */
            }
        }
    }
}