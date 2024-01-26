using System.IO.Abstractions;
using FluentAssertions;
using Moq;
using Serilog.Core;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Log;
using TaskFlux.Consensus.Persistence.Metadata;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Consensus.Tests.Infrastructure;
using TaskFlux.Consensus.Tests.Stubs;
using TaskFlux.Core;

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Raft")]
public class FollowerStateTests
{
    private static readonly NodeId NodeId = new(1);

    private static readonly PeerGroup EmptyPeerGroup = new(Array.Empty<IPeer>());

    private static RaftConsensusModule<int, int> CreateFollowerNode(Term currentTerm,
                                                                    NodeId? votedFor,
                                                                    ITimer? electionTimer = null,
                                                                    IBackgroundJobQueue? jobQueue = null)
    {
        var fs = Helpers.CreateFileSystem();
        var timerFactory = electionTimer != null
                               ? new ConstantTimerFactory(electionTimer)
                               : Helpers.NullTimerFactory;
        var backgroundJobQueue = jobQueue ?? Helpers.NullBackgroundJobQueue;
        var metadataFile = MetadataFile.Initialize(fs.DataDirectory);
        metadataFile.SetupMetadataTest(currentTerm, votedFor);
        var persistence = new FileSystemPersistenceFacade(FileLog.Initialize(fs.DataDirectory),
            metadataFile,
            SnapshotFile.Initialize(fs.DataDirectory), Logger.None);

        var node = new RaftConsensusModule(NodeId,
            EmptyPeerGroup,
            Logger.None,
            timerFactory,
            backgroundJobQueue,
            persistence,
            Helpers.NullDeltaExtractor,
            Helpers.NullApplicationFactory);

        node.SetStateTest(node.CreateFollowerState());

        return node;
    }

    [Fact]
    public void RequestVote__СБолееВысокимТермом()
    {
        var oldTerm = new Term(1);
        var node = CreateFollowerNode(oldTerm, null);

        var expectedTerm = oldTerm.Increment();
        var candidateId = new NodeId(2);

        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(oldTerm, 0));

        var response = node.Handle(request);

        Assert.True(response.VoteGranted);
        Assert.Equal(expectedTerm, node.CurrentTerm);
        Assert.Equal(candidateId, node.VotedFor);
    }

    [Fact]
    public void RequestVote__СРавнымТермомКогдаРанееНеГолосовал__ДолженОтветитьTrue()
    {
        var term = new Term(1);
        var node = CreateFollowerNode(term, null);

        var expectedTerm = term;
        var candidateId = new NodeId(2);

        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(term, 0));

        var response = node.Handle(request);

        Assert.True(response.VoteGranted);
        Assert.Equal(expectedTerm, node.CurrentTerm);
        Assert.Equal(candidateId, node.VotedFor);
    }

    [Fact]
    public void RequestVote__СРавнымТермомКогдаУжеГолосовалЗаУзел__ДолженОтветитьTrue()
    {
        var term = new Term(1);
        var expectedTerm = term;
        var candidateId = new NodeId(2);
        var node = CreateFollowerNode(term, candidateId);

        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(term, 0));

        var response = node.Handle(request);

        Assert.True(response.VoteGranted);
        Assert.Equal(expectedTerm, node.CurrentTerm);
        Assert.Equal(candidateId, node.VotedFor);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(3, 2)]
    [InlineData(3, 1)]
    public void RequestVote__СТермомМеньше__ДолженОтветитьFalse(
        int myTerm,
        int otherTerm)
    {
        var oldTerm = new Term(myTerm);

        var node = CreateFollowerNode(oldTerm, null);

        var expectedTerm = new Term(otherTerm);
        var request = new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(oldTerm, 0));

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__СКонфликтующимЛогом__ДолженОтветитьFalse()
    {
        var term = new Term(4);
        var node = CreateFollowerNode(term, null);
        node.Persistence.InsertRange(new LogEntry[]
        {
            new(new(1), Array.Empty<byte>()), // 1
            new(new(3), Array.Empty<byte>()), // 2
            new(new(3), Array.Empty<byte>()), // 3
        }, 0);
        var expectedTerm = term;
        var candidateId = new NodeId(2);

        // Конфликт на 2 записи (индекс 1): наш терм - 3, его терм - 2
        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(new Term(2), 1));

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);

        // Проверка, что состояние не изменилось
        Assert.Equal(term, node.CurrentTerm);
        Assert.Null(node.VotedFor);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеНоЛогКонфликтует__ДолженОтветитьFalseИИзменитьТерм()
    {
        var term = new Term(4);
        var node = CreateFollowerNode(term, null);
        node.Persistence.InsertRange(new LogEntry[]
        {
            new(new(1), Array.Empty<byte>()), // 1
            new(new(3), Array.Empty<byte>()), // 2
            new(new(3), Array.Empty<byte>()), // 3
        }, 0);
        var expectedTerm = term.Increment();
        var candidateId = new NodeId(2);

        // Конфликт на 2 записи (индекс 1): наш терм - 3, его терм - 2
        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(new Term(2), 1));

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(expectedTerm, node.CurrentTerm);
        Assert.Null(node.VotedFor);
    }

    [Fact]
    public void ElectionTimeout__ДолженПерейтиВСостояниеCandidate()
    {
        var timer = new Mock<ITimer>(MockBehavior.Loose);

        var node = CreateFollowerNode(new Term(1), null, electionTimer: timer.Object);

        timer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
    }

    [Fact]
    public void ElectionTimeout__ПослеСрабатыванияОбработчика__ДолженПерейтиВСледующийТерм()
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var raft = CreateFollowerNode(new Term(1), null, timer.Object);

        timer.Raise(x => x.Timeout += null);

        var expectedTerm = oldTerm.Increment();
        Assert.Equal(expectedTerm, raft.CurrentTerm);
    }

    [Fact]
    public void ElectionTimeout__ПослеСрабатыванияОбработчика__ДолженПроголосоватьЗаСебя()
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var node = CreateFollowerNode(oldTerm, null, timer.Object);

        timer.Raise(x => x.Timeout += null);

        Assert.Equal(node.Id, node.VotedFor);
    }

    [Fact]
    public void AppendEntries__ДолженПерезапуститьElectionTimeout()
    {
        var term = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        timer.Setup(x => x.Stop())
             .Verifiable();
        timer.Setup(x => x.Schedule())
             .Verifiable();

        using var node = CreateFollowerNode(term, null, electionTimer: timer.Object);

        var request = AppendEntriesRequest.Heartbeat(term, 0,
            new NodeId(id: NodeId.Id + 1), new LogEntryInfo(term, 0));

        node.Handle(request);

        timer.Verify(x => x.Stop(), Times.Once());
        // Считаем еще самый первый запуск
        timer.Verify(x => x.Schedule(), Times.Exactly(2));
    }

    [Fact]
    public void Heartbeat__СОдинаковымТермомИОдинаковойПоследнейЗаписью__ДолженОтдатьГолос()
    {
        var term = new Term(3);

        var node = CreateFollowerNode(term, null);

        node.Persistence.InsertRange(
            new LogEntry[]
            {
                new(new(1), Array.Empty<byte>()), new(new(2), Array.Empty<byte>()),
                new(new(4), Array.Empty<byte>()),
            }, 0);

        // Наша последняя запись - (4, 2)
        // Последняя запись кандидата - (4, 2)
        var request = AppendEntriesRequest.Heartbeat(term, 2,
            new NodeId(id: NodeId.Id + 1), new LogEntryInfo(new Term(4), 2));

        var response = node.Handle(request);

        Assert.True(response.Success);
        Assert.Equal(term, node.CurrentTerm);
    }

    [Fact]
    public void RequestVote__СБолееВысокимТермом__ДолженОтдатьГолосЗаКандидата()
    {
        var oldTerm = new Term(1);
        using var raft = CreateFollowerNode(oldTerm, null);

        var candidateId = new NodeId(2);
        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: oldTerm.Increment(),
            LastLogEntryInfo: raft.Persistence.LastEntry);
        raft.Handle(request);

        Assert.Equal(candidateId, raft.VotedFor);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Heartbeat__СБолееВысокимТермом__ДолженВыставитьСвойГолосВnull(int? oldVotedFor)
    {
        var oldTerm = new Term(1);
        NodeId? votedForId = oldVotedFor is null
                                 ? null
                                 : new NodeId(oldVotedFor.Value);

        using var node = CreateFollowerNode(oldTerm, votedForId);

        var request = AppendEntriesRequest.Heartbeat(node.CurrentTerm.Increment(), node.Persistence.CommitIndex,
            new NodeId(2),
            node.Persistence.LastEntry);
        node.Handle(request);

        Assert.False(node.VotedFor.HasValue);
    }

    [Fact]
    public void Heartbeat__СБолееВысокимТермом__ДолженОстатьсяFollower()
    {
        var oldTerm = new Term(1);

        using var node = CreateFollowerNode(oldTerm, null);

        var request = AppendEntriesRequest.Heartbeat(node.CurrentTerm.Increment(), node.Persistence.CommitIndex,
            new NodeId(2),
            node.Persistence.LastEntry);
        node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }


    private record ConsensusFileSystem(IFileInfo SnapshotFile);

    private record NodeCreateResult(
        RaftConsensusModule Module,
        FileSystemPersistenceFacade Persistence,
        ConsensusFileSystem FileSystem);

    private static NodeCreateResult CreateFollowerNodeNew()
    {
        // Follower никому ничего не отправляет, только принимает
        var peers = new PeerGroup(Array.Empty<IPeer>());

        // Follower не использует фоновые задачи - это только для Candidate/Leader
        var backgroundJobQueue = Mock.Of<IBackgroundJobQueue>();
        var deltaBytes = new byte[] {1,};
        var commandSerializer =
            Mock.Of<IDeltaExtractor<int>>(x => x.TryGetDelta(It.IsAny<int>(), out deltaBytes) == true);

        var (persistenceFacade, fileSystem) = CreateStorage();
        var node = new RaftConsensusModule(NodeId,
            peers, Logger.None,
            Helpers.NullTimerFactory,
            backgroundJobQueue,
            persistenceFacade,
            commandSerializer,
            Helpers.NullApplicationFactory);

        node.SetStateTest(node.CreateFollowerState());

        return new NodeCreateResult(node, persistenceFacade, fileSystem);

        (FileSystemPersistenceFacade, ConsensusFileSystem) CreateStorage()
        {
            var (_, _, _, snapshot, _, dataDir) = Helpers.CreateFileSystem();
            var logFile = FileLog.Initialize(dataDir);
            var metadataFile = MetadataFile.Initialize(dataDir);
            var snapshotFile = SnapshotFile.Initialize(dataDir);
            return ( new FileSystemPersistenceFacade(logFile, metadataFile, snapshotFile, Logger.None),
                     new ConsensusFileSystem(snapshot) );
        }
    }

    [Fact]
    public void InstallSnapshot__ДолженСоздатьНовыйФайлСнапшота()
    {
        var (node, persistence, fs) = CreateFollowerNodeNew();
        fs.SnapshotFile.Delete();

        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(2);

        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));
        var response = node.Handle(request);

        response.CurrentTerm
                .Should()
                .Be(leaderTerm, "отправленный лидером терм больше текущего");
        var (index, term, data) = persistence.Snapshot.ReadAllDataTest();
        Assert.Equal(10, index);
        Assert.Equal(new Term(2), term);
        Assert.Equal(snapshotData, data);
    }

    [Fact]
    public void InstallSnapshot__СуществующийПустойФайлСнапшотаДолженПерезаписаться()
    {
        var (node, persistence, _) = CreateFollowerNodeNew();

        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(2);
        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));

        var response = node.Handle(request);
        Assert.Equal(leaderTerm, response.CurrentTerm);

        var (index, term, data) = persistence.Snapshot.ReadAllDataTest();
        Assert.Equal(lastIncludedEntry.Index, index);
        Assert.Equal(lastIncludedEntry.Term, term);
        Assert.Equal(snapshotData, data);
    }

    [Fact]
    public void InstallSnapshot__СуществующийФайлСнапшотаДолженПерезаписаться()
    {
        var (node, persistence, _) = CreateFollowerNodeNew();
        persistence.Snapshot.SetupSnapshotTest(new Term(2), 3,
            new StubSnapshot(new byte[] {9, 5, 234, 1, 6, 2, 44, 2, 7, 45, 52, 97}));

        var lastIncludedEntry = new LogEntryInfo(new Term(4), 13);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(4);
        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));

        var response = node.Handle(request);
        response.CurrentTerm
                .Should()
                .Be(leaderTerm, "терм лидера больше текущего");

        var (index, term, data) = persistence.Snapshot.ReadAllDataTest();
        Assert.Equal(lastIncludedEntry.Index, index);
        Assert.Equal(lastIncludedEntry.Term, term);
        Assert.Equal(snapshotData, data);
        Assert.Equal(lastIncludedEntry, persistence.Snapshot.LastApplied);
    }

    [Fact]
    public void InstallSnapshot__КогдаТермЛидераМеньше__ДолженВернутьОтветСразу()
    {
        var leaderTerm = new Term(4);
        var nodeTerm = leaderTerm.Increment();

        var (node, persistence, _) = CreateFollowerNodeNew();
        persistence.UpdateState(nodeTerm, null);

        var snapshot = new Mock<ISnapshot>().Apply(m =>
        {
            m.Setup(x => x.GetAllChunks(It.IsAny<CancellationToken>()))
             .Returns(Array.Empty<ReadOnlyMemory<byte>>())
             .Verifiable();
        });

        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry, snapshot.Object);

        var response = node.Handle(request);
        response.CurrentTerm
                .Should()
                .Be(nodeTerm, "терм должен быть как у лидера");

        snapshot.Verify(x => x.GetAllChunks(It.IsAny<CancellationToken>()), Times.Never());
    }

    private static readonly NodeId AnotherNodeId = new NodeId(NodeId.Id + 1);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void AppendEntries__КогдаЛогПустой__ДолженВставитьЗаписиВНачало(int entriesCount)
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(term, new[] {( byte ) i}))
                                .ToArray();

        // Лог изначально был пуст и у нас, и у лидера
        // Причем, еще ничего не закоммичено
        var node = CreateFollowerNode(term, null);

        var request = new AppendEntriesRequest(term, LogEntryInfo.TombIndex, AnotherNodeId, LogEntryInfo.Tomb, entries);

        var response = node.Handle(request);

        Assert.True(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаЕстьКонфликтующиеЗаписи__ДолженПерезатеретьНеЗакомиченные()
    {
        var term = new Term(2);
        var node = CreateFollowerNode(term, null);

        // 3 закоммиченные записи с индексами
        var committedEntries = new LogEntry[] {new(new(1), [1]), new(new(2), [2]), new(new(2), [3]),};

        node.Persistence.Log.AppendRange(committedEntries);

        // Добавляем 2 незакоммиченные записи
        node.Persistence.InsertRange(new LogEntry[] {new(new(3), new byte[] {4}), new(new(3), new byte[] {5}),},
            3);

        // В запросе передается 1 запись (идет сразу после наших закоммиченных) 
        var leaderCommit = 2;
        var newEntries = new LogEntry[] {new(new(3), new byte[] {6}), new(new(3), new byte[] {7}),};
        var prevLogEntryInfo = new LogEntryInfo(committedEntries[^1].Term, committedEntries.Length - 1);
        var request = new AppendEntriesRequest(term, leaderCommit, AnotherNodeId, prevLogEntryInfo, newEntries);

        var response = node.Handle(request);

        Assert.True(response.Success);
        var actualEntries = node.Persistence.Log.ReadAllTest();
        Assert.Equal(committedEntries.Concat(newEntries), actualEntries, LogEntryEqualityComparer.Instance);
        Assert.Equal(request.Term, response.Term);
        Assert.Equal(node.CurrentTerm, response.Term);
    }

    [Fact]
    public void RequestVote__КогдаУжеГолосовалВТерме__НеДолженОтдатьГолос()
    {
        var term = new Term(4);
        var votedFor = new NodeId(5);
        var candidateId = new NodeId(2);

        var node = CreateFollowerNode(term, votedFor);
        node.Persistence.InsertRange(new LogEntry[]
        {
            new(new(1), Array.Empty<byte>()), // 1
            new(new(3), Array.Empty<byte>()), // 2
            new(new(3), Array.Empty<byte>()), // 3
        }, 0);
        var expectedTerm = term.Increment();

        // Конфликт на 2 записи (индекс 1): наш терм - 3, его терм - 2
        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(new Term(2), 1));

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Fact]
    public void RequestVote__КогдаУжеОтдавалГолосЗаУзел__ДолженПовторноОтдатьГолос()
    {
        var term = new Term(4);
        var votedFor = new NodeId(5);

        var node = CreateFollowerNode(term, votedFor);
        node.Persistence.InsertRange(new LogEntry[]
        {
            new(new(1), Array.Empty<byte>()), // 1
            new(new(3), Array.Empty<byte>()), // 2
            new(new(3), Array.Empty<byte>()), // 3
        }, 0);
        var expectedTerm = term.Increment();

        // Конфликт на 2 записи (индекс 1): наш терм - 3, его терм - 2
        var request = new RequestVoteRequest(CandidateId: votedFor, CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(new Term(2), 1));

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Fact]
    public void InstallSnapshot__ДолженОчиститьЛогИБуфер()
    {
        var (node, persistence, fs) = CreateFollowerNodeNew();
        fs.SnapshotFile.Delete();

        var lastSnapshotEntry = new LogEntryInfo(new Term(2), 10);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(2);

        var response = node.Handle(new InstallSnapshotRequest(leaderTerm,
            new NodeId(1),
            lastSnapshotEntry,
            new StubSnapshot(snapshotData)));

        response.CurrentTerm
                .Should()
                .Be(leaderTerm, "отправленный лидером терм больше текущего");
        persistence.Log.GetUncommittedTest()
                   .Should()
                   .BeEmpty("лог должен быть очищен после установки нового снапшота");
        persistence.Log.GetCommittedTest()
                   .Should()
                   .BeEmpty("лог должен быть очищен после установки нового снапшота");
    }

    [Fact]
    public void InstallSnapshot__КогдаИндексСнапшотаРавенТекущемуИндексу__ДолженУстановитьСнапшот()
    {
        // Такое может случиться, когда на лидера большая нагрузка идет и отправляется тот же самый снапшот

        var (follower, storage, _) = CreateFollowerNodeNew();
        var snapshot = new StubSnapshot(new byte[] {1, 2, 3});
        var term = new Term(2);
        var logEntryInfo = new LogEntryInfo(new Term(2), 1000);
        var oldSnapshot = new StubSnapshot(new byte[] {4, 5, 6});
        storage.Snapshot.SetupSnapshotTest(logEntryInfo.Term, logEntryInfo.Index, oldSnapshot);

        var request = new InstallSnapshotRequest(term, AnotherNodeId, logEntryInfo, snapshot);
        follower.Handle(request);

        var (lastIndex, lastTerm, snapshotData) = storage.Snapshot.ReadAllDataTest();
        lastIndex.Should()
                 .Be(logEntryInfo.Index, "индекс записи в снапшоте должна быть такая же как и в запросе");
        lastTerm.Should()
                .Be(logEntryInfo.Term, "терм записи в снапшоте должна быть такой же как и в запросе");
        snapshotData
           .Should()
           .Equal(snapshot.Data, "данные снапшота должны быть перезаписаны");
    }
}