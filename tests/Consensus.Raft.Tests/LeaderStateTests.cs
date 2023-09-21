using System.Buffers.Binary;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using Consensus.Raft.Tests.Infrastructure;
using Consensus.Raft.Tests.Stubs;
using FluentAssertions;
using Moq;
using Serilog.Core;
using TaskFlux.Core;

namespace Consensus.Raft.Tests;

[Trait("Category", "Raft")]
public class LeaderStateTests
{
    private static readonly NodeId NodeId = new(1);
    private static readonly NodeId AnotherNodeId = new(NodeId.Id + 1);

    private class IntCommandSerializer : ICommandSerializer<int>
    {
        public byte[] Serialize(int value)
        {
            var result = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(result, value);
            return result;
        }

        public int Deserialize(byte[] payload)
        {
            return BinaryPrimitives.ReadInt32BigEndian(payload);
        }
    }

    private static readonly ICommandSerializer<int> CommandSerializer = new IntCommandSerializer();

    private RaftConsensusModule CreateLeaderNode(Term term,
                                                 NodeId? votedFor,
                                                 ITimer? heartbeatTimer = null,
                                                 IEnumerable<IPeer>? peers = null,
                                                 LogEntry[]? logEntries = null,
                                                 IApplication? application = null)
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
        application ??= Helpers.NullApplication;
        var facade = CreateFacade();
        if (logEntries is {Length: > 0})
        {
            facade.LogStorage.SetFileTest(logEntries);
        }

        var node = new RaftConsensusModule(NodeId, peerGroup, Logger.None, timerFactory,
            Helpers.NullBackgroundJobQueue, facade, application,
            CommandSerializer, Helpers.NullApplicationFactory);
        node.SetStateTest(node.CreateLeaderState());
        return node;

        StoragePersistenceFacade CreateFacade()
        {
            var fs = Helpers.CreateFileSystem();
            var log = new FileLogStorage(fs.Log, fs.TemporaryDirectory);
            var metadata = new FileMetadataStorage(fs.Metadata.Open(FileMode.Open), term, votedFor);
            var snapshotStorage = new FileSystemSnapshotStorage(fs.Snapshot, fs.TemporaryDirectory, Logger.None);
            return new StoragePersistenceFacade(log, metadata, snapshotStorage);
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

        var request = new RequestVoteRequest(CandidateId: AnotherNodeId, CandidateTerm: new(otherTerm),
            LastLogEntryInfo: node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеНоЛогКонфликтует__НеДолженОтдатьГолос()
    {
        var term = new Term(1);
        var existingLog = new[]
        {
            IntDataEntry(term), // 0
            IntDataEntry(term), // 1
            IntDataEntry(term), // 2
            IntDataEntry(term), // 3
        };

        using var node = CreateLeaderNode(term, null, logEntries: existingLog);
        var greaterTerm = term.Increment();
        // Конфликт на 2 индексе - термы расходятся
        var request = new RequestVoteRequest(AnotherNodeId, greaterTerm, new LogEntryInfo(greaterTerm, 2));
        var response = node.Handle(request);

        response.VoteGranted
                .Should()
                .BeFalse("Лог конфликтует - голос не может быть отдан");
        node.CurrentTerm
            .Should()
            .Be(greaterTerm, "Терм в запросе был больше, надо обновить");
        node.CurrentRole
            .Should()
            .Be(NodeRole.Follower, "При получении большего терма нужно стать последователем");
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

        var request = AppendEntriesRequest.Heartbeat(new(otherTerm), node.PersistenceFacade.CommitIndex, AnotherNodeId,
            node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.Success);
    }

    [Fact]
    public void AppendEntries__СБолееВысокимТермом__ДолженСтатьFollower()
    {
        var term = new Term(1);
        using var node = CreateLeaderNode(term, null);
        var expectedTerm = term.Increment();
        var request = AppendEntriesRequest.Heartbeat(expectedTerm, node.PersistenceFacade.CommitIndex,
            AnotherNodeId, node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, response.Term);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Fact]
    public void ПриОтправкеHeartbeat__КогдаУзелОтветилОтрицательноИЕгоТермБольше__ДолженПерейтиВFollower()
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Schedule());
        });

        var peerTerm = term.Increment();

        var peer = CreateDefaultPeer();
        peer.Setup(x => x.SendAppendEntriesAsync(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
             // Первый вызов для Heartbeat
            .ReturnsAsync(new AppendEntriesResponse(peerTerm, false)); // Второй для нас
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>()))
            .Returns(new AppendEntriesResponse(peerTerm, false));
        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object,
            peers: new[] {peer.Object});

        heartbeatTimer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void ПриОтправкеHeartbeat__КогдаУзелБылПуст__ДолженСинхронизироватьЛогПриСтарте()
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
        var existingFileEntries = new[] {IntDataEntry(1), IntDataEntry(1), IntDataEntry(2), IntDataEntry(3),};

        var peer = CreateDefaultPeer();

        // Достигнуто ли начало лога
        var beginReached = false;
        var sentEntries = Array.Empty<LogEntry>();
        peer.Setup(x =>
                 x.SendAppendEntries(It.IsAny<AppendEntriesRequest>()))
            .Returns((AppendEntriesRequest request) =>
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

        using var node =
            CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object, peers: new[] {peer.Object});
        // Выставляем изначальный лог в 4 команды
        node.PersistenceFacade.LogStorage.SetFileTest(existingFileEntries);

        heartbeatTimer.Raise(x => x.Timeout += null);

        beginReached.Should()
                    .BeTrue("Окончание репликации должно быть закончено, когда достигнуто начало лога");
        sentEntries.Should()
                   .BeEquivalentTo(existingFileEntries, options => options.Using(LogEntryComparer),
                        "Отправленные записи должны полностью соответствовать логу");
    }

    [Fact]
    public void ПриОтправкеHeartbeat__КогдаУзелБылНеПолностьюПуст__ДолженСинхронизироватьЛог()
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
        var existingFileEntries = new[] {IntDataEntry(1), IntDataEntry(1), IntDataEntry(2), IntDataEntry(3),};
        // В логе узла есть все записи до 2 (индекс = 1)
        var storedEntriesIndex = 1;

        var expectedSent = existingFileEntries
                          .Skip(storedEntriesIndex + 1)
                          .ToArray();

        var peer = CreateDefaultPeer();

        // Достигнуто ли начало лога
        var beginReached = false;
        // Отправленные записи при достижении
        var sentEntries = Array.Empty<LogEntry>();
        peer.Setup(x =>
                 x.SendAppendEntries(It.IsAny<AppendEntriesRequest>()))
            .Returns((AppendEntriesRequest request) =>
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

        using var node = CreateLeaderNode(term, null,
            heartbeatTimer: heartbeatTimer.Object, peers: new[] {peer.Object},
            logEntries: existingFileEntries);

        heartbeatTimer.Raise(x => x.Timeout += null);

        beginReached.Should()
                    .BeTrue("Окончание репликации должно быть закончено, когда достигнуто начало лога");
        sentEntries.Should()
                   .BeEquivalentTo(expectedSent, options => options.Using(LogEntryComparer),
                        "Отправленные записи должны полностью соответствовать логу");
    }

    private static LogEntry IntDataEntry(Term term)
    {
        var data = new byte[Random.Shared.Next(0, 128)];
        Random.Shared.NextBytes(data);
        return new LogEntry(term, data);
    }

    private static LogEntry IntDataEntry(int term) => IntDataEntry(new Term(term));

    [Fact]
    public void AppendEntries__КогдаТермБольше__ДолженДобавитьЗаписиВЛог()
    {
        var term = new Term(3);
        using var node = CreateLeaderNode(term, null);
        var expectedTerm = term.Increment();
        var entries = new[] {IntDataEntry(1), IntDataEntry(2), IntDataEntry(2), IntDataEntry(3),};
        var request = new AppendEntriesRequest(expectedTerm, LogEntryInfo.Tomb.Index, AnotherNodeId, LogEntryInfo.Tomb,
            entries);
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, node.CurrentTerm);

        var actualEntries = node.PersistenceFacade.ReadLogBufferTest();
        Assert.Equal(entries, actualEntries);
    }

    private static readonly LogEntryEqualityComparer LogEntryComparer = LogEntryEqualityComparer.Instance;

    private static LogEntry IntLogEntry(Term term)
    {
        var buffer = new byte[4];
        Random.Shared.NextBytes(buffer);
        return new LogEntry(term, buffer);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(5, 0)]
    [InlineData(5, 1)]
    [InlineData(5, 2)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void AppendEntries__КогдаТермБольше__ДолженЗакоммититьЗаписи(int bufferSize, int index)
    {
        var term = new Term(3);
        var expectedTerm = term.Increment();
        var nodeEntries = Enumerable.Range(0, bufferSize)
                                    .Select(_ => IntLogEntry(term))
                                    .ToArray();
        var (expectedFile, expectedBuffer) = nodeEntries.Split(index);
        using var node = CreateLeaderNode(term, null);
        node.PersistenceFacade.SetupBufferTest(nodeEntries);
        var request = new AppendEntriesRequest(expectedTerm, index, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>());
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedTerm, response.Term);

        var actualFile = node.PersistenceFacade.ReadLogFileTest();
        Assert.Equal(expectedFile, actualFile, LogEntryComparer);
        var actualBuffer = node.PersistenceFacade.ReadLogBufferTest();
        Assert.Equal(expectedBuffer, actualBuffer, LogEntryComparer);
    }

    private static SubmitRequest<int> CreateSubmitRequest(int value = 0) =>
        new(new CommandDescriptor<int>(value, false));

    [Fact]
    public void SubmitRequest__КогдаДругихУзловНет__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var mock = new Mock<IApplication>();
        var command = 1;
        var expectedResponse = 123;
        var request = CreateSubmitRequest(command);
        mock.Setup(x => x.Apply(It.Is<int>(y => y == command)))
            .Returns(expectedResponse)
            .Verifiable();
        using var node = CreateLeaderNode(term, null, peers: Array.Empty<IPeer>(), application: mock.Object);

        var response = node.Handle(request);

        Assert.Equal(expectedResponse, response.Response);
        var committedEntry = node.PersistenceFacade.ReadLogFileTest().Single();
        AssertCommandEqual(committedEntry, command);
        mock.Verify(x => x.Apply(It.Is<int>(y => y == command)), Times.Once());
    }

    private static void AssertCommandEqual(LogEntry entry, int expected)
    {
        Assert.Equal(expected, CommandSerializer.Deserialize(entry.Data));
    }

    [Fact(Timeout = 1000)]
    public void SubmitRequest__КогдаЕдинственныйДругойУзелОтветилУспехом__ДолженОбработатьЗапрос()
    {
        var term = new Term(1);
        var machine = new Mock<IApplication>();
        var command = 1;
        var expectedResponse = 123;
        var peer = CreateDefaultPeer();
        peer.Setup(x => x.SendAppendEntriesAsync(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse(term, true))
            .Verifiable();
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>()))
            .Returns(new AppendEntriesResponse(term, true))
            .Verifiable();
        var request = CreateSubmitRequest(command);
        machine.Setup(x => x.Apply(It.Is<int>(y => y == command)))
               .Returns(expectedResponse)
               .Verifiable();

        using var node = CreateLeaderNode(term, null,
            peers: new[] {peer.Object},
            application: machine.Object);

        var response = node.Handle(request);

        Assert.Equal(expectedResponse, response.Response);
        var committedEntry = node.PersistenceFacade.ReadLogFileTest().Single();
        AssertCommandEqual(committedEntry, command);
        machine.Verify(x => x.Apply(It.Is<int>(y => y == command)), Times.Once());
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
                                       m.Setup(x => x.SendAppendEntriesAsync(It.IsAny<AppendEntriesRequest>(),
                                             It.IsAny<CancellationToken>()))
                                        .ReturnsAsync(AppendEntriesResponse.Ok(term));
                                       m.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>()))
                                        .Returns(AppendEntriesResponse.Ok(term));
                                   }))
                              .ToArray(peersCount);

        using var node = CreateLeaderNode(term, null, peers: peers.Select(x => x.Object));
        var request = new SubmitRequest<int>(new CommandDescriptor<int>(123, false));
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
        peer.Setup(x => x.SendAppendEntriesAsync(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AppendEntriesResponse(greaterTerm, false));
        peer.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>()))
            .Returns(new AppendEntriesResponse(greaterTerm, false));
        using var node = CreateLeaderNode(term, null, peers: new[] {peer.Object});
        var request = new SubmitRequest<int>(new CommandDescriptor<int>(123, false));
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
}