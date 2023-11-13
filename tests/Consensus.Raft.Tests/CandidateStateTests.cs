using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.LogFileCheckStrategy;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using Consensus.Raft.Tests.Infrastructure;
using Consensus.Raft.Tests.Stubs;
using FluentAssertions;
using Moq;
using Serilog.Core;
using TaskFlux.Models;

namespace Consensus.Raft.Tests;

[Trait("Category", "Raft")]
public class CandidateStateTests
{
    private static readonly NodeId NodeId = new(1);
    private static readonly PeerGroup EmptyPeerGroup = new(Array.Empty<IPeer>());
    private static readonly IApplication NullApplication = Mock.Of<IApplication>();
    private static readonly IApplicationFactory NullApplicationFactory = Mock.Of<IApplicationFactory>();

    private static readonly ICommandSerializer<int> NullCommandSerializer =
        new Mock<ICommandSerializer<int>>().Apply(m =>
                                            {
                                                m.Setup(x => x.Serialize(It.IsAny<int>())).Returns(Array.Empty<byte>());
                                                m.Setup(x => x.Deserialize(It.IsAny<byte[]>())).Returns(1);
                                            })
                                           .Object;

    private static RaftConsensusModule CreateCandidateNode(Term term,
                                                           ITimer? electionTimer = null,
                                                           IApplication? application = null,
                                                           IBackgroundJobQueue? jobQueue = null,
                                                           IEnumerable<IPeer>? peers = null,
                                                           ILogFileSizeChecker? fileSizeChecker = null)
    {
        return CreateCandidateNode(term.Value, electionTimer, application, jobQueue, peers, fileSizeChecker);
    }

    private static RaftConsensusModule CreateCandidateNode(int term,
                                                           ITimer? electionTimer = null,
                                                           IApplication? application = null,
                                                           IBackgroundJobQueue? jobQueue = null,
                                                           IEnumerable<IPeer>? peers = null,
                                                           ILogFileSizeChecker? fileSizeChecker = null)
    {
        var facade = CreateStoragePersistenceFacade();
        electionTimer ??= Mock.Of<ITimer>();
        var timerFactory = electionTimer is null
                               ? Helpers.NullTimerFactory
                               : new ConstantTimerFactory(electionTimer);
        jobQueue ??= Mock.Of<IBackgroundJobQueue>();
        var peerGroup = peers != null
                            ? new PeerGroup(peers.ToArray())
                            : EmptyPeerGroup;
        application ??= NullApplication;
        var node = new RaftConsensusModule(NodeId, peerGroup,
            Logger.None, timerFactory, jobQueue,
            facade,
            application, NullCommandSerializer, NullApplicationFactory);
        node.SetStateTest(node.CreateCandidateState());
        return node;

        StoragePersistenceFacade CreateStoragePersistenceFacade()
        {
            var fs = Helpers.CreateFileSystem();
            var logStorage = new FileLogStorage(fs.Log, fs.TemporaryDirectory);
            var metadataStorage =
                new FileMetadataStorage(fs.Metadata.Open(FileMode.Open), new Term(term), NodeId);
            var snapshotStorage = new FileSystemSnapshotStorage(fs.Snapshot, fs.TemporaryDirectory, Logger.None);
            if (fileSizeChecker is null)
            {
                return new StoragePersistenceFacade(logStorage, metadataStorage, snapshotStorage);
            }

            return new StoragePersistenceFacade(logStorage, metadataStorage, snapshotStorage, fileSizeChecker);
        }
    }

    private const int DefaultTerm = 1;

    [Fact]
    public void ElectionTimout__ДолженПерейтиВСледующийТермИОстатьсяКандидатом()
    {
        var electionTimer = new Mock<ITimer>();
        electionTimer.SetupAdd(x => x.Timeout += null);
        var currentTerm = new Term(1);
        var expectedTerm = currentTerm.Increment();
        var node = CreateCandidateNode(DefaultTerm, electionTimer.Object);

        electionTimer.Raise(x => x.Timeout += null);

        Assert.Equal(expectedTerm, node.CurrentTerm);
        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
    }

    [Fact]
    public void Кворум__КогдаДругихУзловНет__ДолженСтатьЛидером()
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(oldTerm.Value, jobQueue: jobQueue);

        jobQueue.Run();

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Fact]
    public void Кворум__ПриДостижении__ДолженОстановитьElectionTimeout()
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunBackgroundJobQueue();
        var timer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Stop()).Verifiable();
        });

        using var _ = CreateCandidateNode(oldTerm.Value, electionTimer: timer.Object, jobQueue: jobQueue);

        jobQueue.Run();

        timer.Verify(x => x.Stop(), Times.Once());
    }

    [Fact]
    public void Кворум__КогдаЕдинственныйДругойУзелНеОтдалГолос__ДолженОстатьсяКандидатом()
    {
        var queue = new SingleRunBackgroundJobQueue();
        var term = new Term(DefaultTerm);
        using var node = CreateCandidateNode(term, jobQueue: queue,
            peers: new[] {new StubQuorumPeer(new RequestVoteResponse(term, false))});

        queue.Run();

        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
    }

    private static readonly NodeId AnotherNodeId = new(NodeId.Id + 1);

    private class StubQuorumPeer : IPeer
    {
        private readonly RequestVoteResponse? _response;
        public NodeId Id => AnotherNodeId;

        public StubQuorumPeer(RequestVoteResponse? response)
        {
            _response = response;
        }

        public StubQuorumPeer(Term term, bool voteGranted)
        {
            _response = new RequestVoteResponse(term, voteGranted);
        }

        public Task<AppendEntriesResponse?> SendAppendEntriesAsync(AppendEntriesRequest request,
                                                                   CancellationToken token)
        {
            throw new Exception("Кандидат не должен отсылать AppendEntries");
        }

        public AppendEntriesResponse SendAppendEntries(AppendEntriesRequest request)
        {
            throw new Exception("Кандидат не должен отсылать AppendEntries");
        }

        public Task<RequestVoteResponse?> SendRequestVoteAsync(RequestVoteRequest request, CancellationToken token)
        {
            return Task.FromResult(_response);
        }

        public RequestVoteResponse? SendRequestVote(RequestVoteRequest request)
        {
            return _response;
        }

        public IEnumerable<InstallSnapshotResponse?> SendInstallSnapshot(
            InstallSnapshotRequest request,
            CancellationToken token)
        {
            throw new Exception("Кандидат не должен отсылать InstallSnapshot");
        }
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(1, 1)]
    [InlineData(2, 0)]
    [InlineData(2, 2)]
    [InlineData(2, 1)]
    [InlineData(3, 1)]
    [InlineData(3, 2)]
    [InlineData(3, 3)]
    [InlineData(5, 5)]
    public void Кворум__СНесколькимиУзлами__ДолженСтатьЛидеромКогдаСобралКворум(
        int grantedVotesCount,
        int nonGrantedVotesCount)
    {
        // Кворум достигается только если было получено n/2 согласий (округление в нижнюю сторону),
        // где n - кол-во других узлов (без нас)
        var term = new Term(2);
        var peers = Enumerable.Range(0, grantedVotesCount)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .Concat(Enumerable.Range(0, nonGrantedVotesCount)
                                                .Select(_ => new StubQuorumPeer(term, false)));

        var queue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: peers);

        queue.Run();

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(0, 2)]
    [InlineData(1, 2)]
    [InlineData(1, 3)]
    [InlineData(2, 3)]
    [InlineData(4, 5)]
    [InlineData(1, 4)]
    [InlineData(2, 4)]
    [InlineData(3, 4)]
    public void Кворум__СНесколькимиУзлами__ДолженОстатьсяКандидатомЕслиНеСобралКворум(
        int grantedVotesCount,
        int nonGrantedVotesCount)
    {
        var term = new Term(2);
        var peers = Enumerable.Range(0, grantedVotesCount)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .Concat(Enumerable.Range(0, nonGrantedVotesCount)
                                                .Select(_ => new StubQuorumPeer(term, false)));

        var queue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: peers);

        queue.Run();

        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
    }

    [Fact]
    public void Кворум__СЕдинственнымУзломКоторыйНеОтветил__ДолженСделатьПовторнуюПопытку()
    {
        var term = new Term(2);
        var mock = new Mock<IPeer>().Apply(m =>
        {
            m.SetupGet(x => x.Id).Returns(AnotherNodeId);
            m.SetupSequence(x => x.SendRequestVoteAsync(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(( RequestVoteResponse? ) null)
             .ReturnsAsync(new RequestVoteResponse(term, true));
        });
        var queue = new SingleRunBackgroundJobQueue();
        using var _ = CreateCandidateNode(term, jobQueue: queue, peers: new[] {mock.Object});

        queue.Run();

        // Первый раз ошибка сети, а второй раз соединение установлено
        mock.Verify(x => x.SendRequestVoteAsync(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public void Кворум__КогдаУзелОтветилБольшимТермомИНеОтдалГолос__ДолженСтатьFollower()
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();
        var newTerm = term.Increment();
        var peer = new StubQuorumPeer(new RequestVoteResponse(newTerm, false));
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: new[] {peer});

        queue.Run();

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void Кворум__КогдаУзелОтветилБольшимТермомИНеОтдалГолос__ДолженОбноситьТерм()
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();
        var newTerm = term.Increment();
        var peer = new StubQuorumPeer(new RequestVoteResponse(newTerm, false));
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: new[] {peer});

        queue.Run();

        Assert.Equal(newTerm, node.CurrentTerm);
    }

    [Fact]
    public void Кворум__СЕдинственнымУзломКоторыйОтдалГолосНаВторойПопытке__ДолженСтатьЛидером()
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();
        var peer = new Mock<IPeer>().Apply(m =>
        {
            m.SetupSequence(x => x.SendRequestVoteAsync(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(( RequestVoteResponse? ) null)
             .ReturnsAsync(new RequestVoteResponse(term, true));
        });
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: new[] {peer.Object});

        queue.Run();

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Fact]
    public void Кворум__КогдаСобранНоОдинИзОтветовИмеетБольшийТерм__ДолженСтатьFollowerВЭтомТерме()
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();
        var agreedNode = new Mock<IPeer>().Apply(m =>
        {
            m.Setup(x => x.SendRequestVoteAsync(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(new RequestVoteResponse(term, true));
        });
        var greaterTerm = term.Increment();
        var greaterNode = new Mock<IPeer>().Apply(m =>
        {
            m.Setup(x => x.SendRequestVoteAsync(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(new RequestVoteResponse(greaterTerm, false));
        });
        using var node =
            CreateCandidateNode(term, jobQueue: queue, peers: new[] {agreedNode.Object, greaterNode.Object});

        queue.Run();

        node.CurrentRole
            .Should()
            .Be(NodeRole.Follower,
                 "узел должен стать последователем, если хотя бы один узел вернул больший терм во время кворума, даже если большинство согласилось");
        node.CurrentTerm
            .Should()
            .Be(greaterTerm, "надо перейти в больший терм, если был обнаружен");
    }

    [Fact]
    public void RequestVote__СБолееВысокимТермом__ДолженСтатьFollower()
    {
        var term = new Term(2);
        using var node = CreateCandidateNode(term);
        var newTerm = term.Increment();
        var request = new RequestVoteRequest(AnotherNodeId, newTerm, LogEntryInfo.Tomb);
        var response = node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.VoteGranted);
        Assert.Equal(newTerm, response.CurrentTerm);
    }

    [Fact]
    public void RequestVote__СБолееВысокимТермом__ДолженОбновитьСвойТерм()
    {
        var term = new Term(2);
        using var node = CreateCandidateNode(term);
        var newTerm = term.Increment();
        var request = new RequestVoteRequest(AnotherNodeId, newTerm, LogEntryInfo.Tomb);
        var response = node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.VoteGranted);
        Assert.Equal(newTerm, response.CurrentTerm);
        Assert.Equal(newTerm, node.CurrentTerm);
    }

    [Fact]
    public void Heartbeat__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var term = new Term(2);
        using var node = CreateCandidateNode(term);
        var newTerm = term.Increment();
        var request = AppendEntriesRequest.Heartbeat(newTerm, node.PersistenceFacade.CommitIndex, AnotherNodeId,
            node.PersistenceFacade.LastEntry);
        var response = node.Handle(request);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.Success);
        Assert.Equal(newTerm, response.Term);
        Assert.Equal(newTerm, node.CurrentTerm);
    }

    private static LogEntry RandomDataEntry(Term term)
    {
        var data = new byte[Random.Shared.Next(0, 128)];
        Random.Shared.NextBytes(data);
        return new LogEntry(term, data);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    public void AppendEntries__ВКонецПустогоЛога__ДолженДобавитьЗаписи(int entriesCount)
    {
        var term = new Term(2);
        using var node = CreateCandidateNode(term);

        var entries = Enumerable.Range(0, entriesCount)
                                .Select(_ => RandomDataEntry(term))
                                .ToArray();
        var request = new AppendEntriesRequest(term, LogEntryInfo.TombIndex, AnotherNodeId, LogEntryInfo.Tomb, entries);
        var response = node.Handle(request);
        Assert.True(response.Success);

        var buffer = node.PersistenceFacade.ReadLogBufferTest();
        Assert.Equal(entries, buffer);
        Assert.Empty(node.PersistenceFacade.ReadLogFileTest());
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(1, 10)]
    [InlineData(5, 1)]
    [InlineData(5, 2)]
    [InlineData(10, 10)]
    public void AppendEntries__ВКонецНеПустогоЛога__ДолженДобавитьЗаписи(int logSize, int entriesCount)
    {
        var term = new Term(2);
        using var node = CreateCandidateNode(term);

        var entries = Enumerable.Range(0, entriesCount)
                                .Select(_ => RandomDataEntry(term))
                                .ToArray();
        var bufferEntries = Enumerable.Range(0, logSize)
                                      .Select(_ => RandomDataEntry(term))
                                      .ToArray();
        node.PersistenceFacade.SetupBufferTest(bufferEntries);
        var request = new AppendEntriesRequest(term, LogEntryInfo.TombIndex, AnotherNodeId,
            node.PersistenceFacade.LastEntry, entries);
        var expectedBuffer = bufferEntries.Concat(entries);

        var response = node.Handle(request);

        Assert.True(response.Success);
        var actualBuffer = node.PersistenceFacade.ReadLogBufferTest();
        Assert.Equal(expectedBuffer, actualBuffer);
        Assert.Empty(node.PersistenceFacade.ReadLogFileTest());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(9)]
    public void AppendEntries__КогдаИндексКоммитаВЗапросеБольшеМоего__ДолженЗакоммититьЗаписи(int commitIndex)
    {
        // Изначально индекс коммита - -1 (ничего не закоммичено)
        var term = new Term(2);
        var bufferEntries = Enumerable.Range(0, 10)
                                      .Select(_ => RandomDataEntry(term))
                                      .ToArray();

        var (expectedFile, expectedBuffer) = bufferEntries.Split(commitIndex);
        using var node = CreateCandidateNode(term);
        node.PersistenceFacade.SetupBufferTest(bufferEntries);

        var request = new AppendEntriesRequest(term, commitIndex, AnotherNodeId, node.PersistenceFacade.LastEntry,
            Array.Empty<LogEntry>());
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedBuffer, node.PersistenceFacade.ReadLogBufferTest(), LogEntryComparer);
        Assert.Equal(expectedFile, node.PersistenceFacade.ReadLogFileTest(), LogEntryComparer);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(0, 5)]
    [InlineData(5, 2)]
    public void AppendEntries__КогдаЕстьИндексКоммитаИЗаписиДляДобавления__ДолженЗакоммититьИДобавитьЗаписи(
        int commitIndex,
        int enqueueCount)
    {
        // Изначально индекс коммита - -1 (ничего не закоммичено)
        // Изначально есть 10 записей
        var term = new Term(2);
        var bufferEntries = Enumerable.Range(0, 10)
                                      .Select(_ => RandomDataEntry(term))
                                      .ToArray();
        var enqueueEntries = Enumerable.Range(0, enqueueCount)
                                       .Select(_ => RandomDataEntry(term))
                                       .ToArray();
        var (expectedFile, expectedBufferEnqueued) = bufferEntries.Split(commitIndex);
        var expectedBuffer = expectedBufferEnqueued.Concat(enqueueEntries);

        using var node = CreateCandidateNode(term);
        node.PersistenceFacade.SetupBufferTest(bufferEntries);

        var request = new AppendEntriesRequest(term, commitIndex, AnotherNodeId, node.PersistenceFacade.LastEntry,
            enqueueEntries);
        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(expectedBuffer, node.PersistenceFacade.ReadLogBufferTest(), LogEntryComparer);
        Assert.Equal(expectedFile, node.PersistenceFacade.ReadLogFileTest(), LogEntryComparer);
    }

    private static readonly LogEntryEqualityComparer LogEntryComparer = new();

    [Fact]
    public void AppendEntries__КогдаЛогКонфликтует__ДолженОтветитьFalse()
    {
        var term = new Term(5);
        using var node = CreateCandidateNode(term);
        var nodeEntries = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(2), // 2
            RandomDataEntry(3), // 3
            RandomDataEntry(3), // 4
            RandomDataEntry(4), // 5
        };
        node.PersistenceFacade.SetupBufferTest(nodeEntries);
        /*
         * Конфликт на 5 записи (индекс 4).
         * Наш терм: 3
         * Терм узла: 4
         */
        var prevLogEntry = new LogEntryInfo(new Term(4), 4);
        var enqueueEntries = new[]
        {
            RandomDataEntry(new Term(4)), RandomDataEntry(new Term(4)), RandomDataEntry(new Term(5)),
        };
        var request = new AppendEntriesRequest(term, 0, AnotherNodeId, prevLogEntry, enqueueEntries);

        var response = node.Handle(request);

        Assert.False(response.Success);
        Assert.Equal(nodeEntries, node.PersistenceFacade.ReadLogFullTest());
    }

    [Fact]
    public void AppendEntries__КогдаРазмерЛогаПревысилМаксимальный__ДолженСоздатьСнапшот()
    {
        var term = new Term(5);
        var existingEntries = new[]
        {
            RandomDataEntry(1), // 0
            RandomDataEntry(2), // 1
            RandomDataEntry(2), // 2
            RandomDataEntry(3), // 3
            RandomDataEntry(5), // 4
        };

        var snapshotData = new byte[100].Apply(arr => Random.Shared.NextBytes(arr));
        var application = new Mock<IApplication>();
        application.Setup(x => x.GetSnapshot())
                   .Returns(new StubSnapshot(snapshotData));
        using var node = CreateCandidateNode(term,
            fileSizeChecker: StubFileSizeChecker.Exceeded,
            application: application.Object);
        node.PersistenceFacade.SetupBufferTest(existingEntries);
        var expectedLastIndex = 4;
        var expectedLastTerm = 5;

        // Этим запросом закоммитим сразу все записи
        var request = new AppendEntriesRequest(term, 4, AnotherNodeId, node.PersistenceFacade.LastEntry,
            Array.Empty<LogEntry>());

        var response = node.Handle(request);

        response.Success
                .Should()
                .BeTrue("Команда полностью допустима - она должна быть применена");

        var (actualIndex, actualTerm, storedData) = node.PersistenceFacade.SnapshotStorage.ReadAllDataTest();

        actualIndex.Should()
                   .Be(expectedLastIndex,
                        "Последний индекс команды снапшота должен равняться последнему индексу примененной команды");
        actualTerm.Value
                  .Should()
                  .Be(expectedLastTerm,
                       "Последний терм команды снапшота должен равняться последнему терму примененной команды");
        node.PersistenceFacade
            .ReadLogFullTest()
            .Should()
            .BeEmpty("После создания снапшота лог должен быть пуст");

        storedData.Should()
                  .BeEquivalentTo(snapshotData,
                       "Данные из файла снапшота должны быть идентичны тем, что передает снапшот");
    }

    private LogEntry RandomDataEntry(int term) => RandomDataEntry(new Term(term));

    [Fact]
    public void RequestVote__СБолееВысокимТермомНоКонфликтующимЛогом__ДолженПерейтиВНовыйТермИНеОтдатьГолос()
    {
        var currentTerm = new Term(2);
        var queue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(currentTerm, jobQueue: queue);
        var nodeEntries = new[] {RandomDataEntry(1), RandomDataEntry(2), RandomDataEntry(2), RandomDataEntry(2),};
        node.PersistenceFacade.SetupBufferTest(nodeEntries);
        var newTerm = currentTerm.Increment();

        // Конфликт на 1 индексе (2 запись) - наш терм = 2, его терм = 1
        var request = new RequestVoteRequest(AnotherNodeId, newTerm, new LogEntryInfo(new Term(1), 1));
        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(newTerm, node.CurrentTerm);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void Heartbeat__СБолееВысокимТермомИСобраннымКворумом__ДолженСтатьFollower()
    {
        /*
         * Когда сначала вызываю Handle (после которого становлюсь Follower),
         * а потом начинаю кворум (собирая при этом большинство голосов),
         * должен остаться Follower
         */
        var currentTerm = new Term(2);
        var queue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(currentTerm, jobQueue: queue);
        var newTerm = currentTerm.Increment();

        var request = AppendEntriesRequest.Heartbeat(newTerm, -1, AnotherNodeId, LogEntryInfo.Tomb);
        var response = node.Handle(request);
        queue.Run();

        Assert.True(response.Success);
        Assert.Equal(newTerm, response.Term);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.Equal(newTerm, node.CurrentTerm);
    }

    [Fact]
    public void RequestVote__СОдинаковымТермом__ДолженВернутьFalse()
    {
        var currentTerm = new Term(2);
        var queue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(currentTerm, jobQueue: queue);

        var request = new RequestVoteRequest(AnotherNodeId, currentTerm, LogEntryInfo.Tomb);
        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(currentTerm, response.CurrentTerm);

        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
        Assert.Equal(currentTerm, node.CurrentTerm);
    }

    [Fact]
    public void СобранныйКворумПослеПолученногоRequestVoteСБольшимТермом__ДолженОстатьсяFollower()
    {
        /*
         * Когда сначала вызываю Handle (после которого становлюсь Follower),
         * а потом начинаю кворум (собирая при этом большинство голосов),
         * должен остаться Follower
         */
        var currentTerm = new Term(2);
        var queue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(currentTerm, jobQueue: queue);
        var newTerm = currentTerm.Increment();

        var request = new RequestVoteRequest(AnotherNodeId, newTerm, LogEntryInfo.Tomb);
        var response = node.Handle(request);

        queue.Run();

        Assert.True(response.VoteGranted);
        Assert.Equal(newTerm, response.CurrentTerm);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.Equal(newTerm, node.CurrentTerm);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    public void ПослеПереходаВLeader__КогдаКворумСобран__ДолженОстановитьElectionТаймер(int votes)
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();

        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendRequestVoteAsync(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RequestVoteResponse(CurrentTerm: term, VoteGranted: true));

        var electionTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        var peers = Enumerable.Range(0, votes)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .ToArray();

        using var _ = CreateCandidateNode(term, electionTimer: electionTimer.Object,
            jobQueue: queue, peers: peers);

        queue.Run();

        electionTimer.Verify(x => x.Stop(), Times.Once());
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 1)]
    [InlineData(3, 2)]
    [InlineData(3, 3)]
    [InlineData(4, 2)]
    [InlineData(5, 1)]
    [InlineData(5, 2)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    [InlineData(5, 5)]
    public void Кворум__КогдаБольшинствоГолосовОтданоДругиеНеОтветили__ДолженПерейтиВLeader(
        int successResponses,
        int notResponded)
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();

        var peers = Enumerable.Range(0, successResponses)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .Concat(Enumerable.Range(0, notResponded)
                                                .Select(_ => new StubQuorumPeer(null)))
                              .ToArray();

        using var node = CreateCandidateNode(term, jobQueue: queue, peers: peers);

        queue.Run();

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }
}