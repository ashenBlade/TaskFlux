using System.IO.Abstractions;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.Persistence.Snapshot;
using Consensus.Raft.State;
using Consensus.Raft.State.LeaderState;
using Consensus.Raft.Tests.Infrastructure;
using Moq;
using Serilog.Core;
using TaskFlux.Core;

namespace Consensus.Raft.Tests;

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
        electionTimer ??= Helpers.NullTimer;

        var backgroundJobQueue = jobQueue ?? Helpers.NullBackgroundJobQueue;

        var persistence = new StoragePersistenceFacade(new FileLogStorage(fs.Log.Open(FileMode.OpenOrCreate)),
            new FileMetadataStorage(fs.Metadata.Open(FileMode.OpenOrCreate), currentTerm, votedFor),
            new FileSystemSnapshotStorage(fs.Snapshot, fs.TemporaryDirectory));

        var node = new RaftConsensusModule(NodeId,
            EmptyPeerGroup,
            Logger.None,
            electionTimer,
            Helpers.NullTimer,
            backgroundJobQueue,
            persistence,
            Helpers.NullCommandQueue,
            Helpers.NullStateMachine,
            Helpers.NullCommandSerializer,
            Helpers.NullRequestQueueFactory,
            Helpers.NullStateMachineFactory);
        node.SetStateTest(new FollowerState<int, int>(node, Helpers.NullStateMachineFactory,
            Helpers.NullCommandSerializer, Logger.None));

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

        var stateMachine = CreateFollowerNode(oldTerm, null);

        var expectedTerm = new Term(otherTerm);
        var request = new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(oldTerm, 0));

        var response = stateMachine.Handle(request);

        Assert.False(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__СКонфликтующимЛогом__ДолженОтветитьFalse()
    {
        var term = new Term(4);
        var node = CreateFollowerNode(term, null);
        node.PersistenceFacade.InsertRange(new LogEntry[]
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
        node.PersistenceFacade.InsertRange(new LogEntry[]
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
    public void RequestVote__ДолженПерезапуститьElectionTimeout()
    {
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Reset()).Verifiable();
        var stateMachine = CreateFollowerNode(new Term(1), null, electionTimer: timer.Object);

        stateMachine.Handle(new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: new Term(1),
            LastLogEntryInfo: new LogEntryInfo(new(1), 0)));

        timer.Verify(x => x.Reset(), Times.Once());
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
        using var stateMachine = CreateFollowerNode(oldTerm, null, timer.Object);

        timer.Raise(x => x.Timeout += null);

        Assert.Equal(stateMachine.Id, stateMachine.VotedFor);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Heartbeat__ДолженСбрасыватьElectionTimeout(int term)
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);

        timer.Setup(x => x.Reset()).Verifiable();

        using var node = CreateFollowerNode(oldTerm, null, electionTimer: timer.Object);

        var request = AppendEntriesRequest.Heartbeat(new Term(term), 0,
            new NodeId(id: NodeId.Id + 1), new LogEntryInfo(new Term(term), 0));

        node.Handle(request);

        timer.Verify(x => x.Reset(), Times.Once());
    }

    [Fact]
    public void Heartbeat__СОдинаковымТермомИОдинаковойПоследнейЗаписью__ДолженОтдатьГолос()
    {
        var term = new Term(3);

        var node = CreateFollowerNode(term, null);

        node.PersistenceFacade.InsertRange(
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
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        timer.Setup(x => x.Reset()).Verifiable();
        using var raft = CreateFollowerNode(oldTerm, null, electionTimer: timer.Object);

        var candidateId = new NodeId(2);
        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: oldTerm.Increment(),
            LastLogEntryInfo: raft.PersistenceFacade.LastEntry);
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

        var request = AppendEntriesRequest.Heartbeat(node.CurrentTerm.Increment(), node.PersistenceFacade.CommitIndex,
            new NodeId(2),
            node.PersistenceFacade.LastEntry);
        node.Handle(request);

        Assert.False(node.VotedFor.HasValue);
    }

    [Fact]
    public void Heartbeat__СБолееВысокимТермом__ДолженОстатьсяFollower()
    {
        var oldTerm = new Term(1);

        using var node = CreateFollowerNode(oldTerm, null);

        var request = AppendEntriesRequest.Heartbeat(node.CurrentTerm.Increment(), node.PersistenceFacade.CommitIndex,
            new NodeId(2),
            node.PersistenceFacade.LastEntry);
        node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }


    private record ConsensusFileSystem(IFileInfo LogFile, IFileInfo MetadataFile, IFileInfo SnapshotFile);

    private record NodeCreateResult(RaftConsensusModule Module,
                                    StoragePersistenceFacade Persistence,
                                    ConsensusFileSystem FileSystem);

    private static NodeCreateResult CreateFollowerNodeNew()
    {
        // Follower никому ничего не отправляет, только принимает
        var peers = new PeerGroup(Array.Empty<IPeer>());

        // Follower не использует фоновые задачи - это только для Candidate/Leader
        var backgroundJobQueue = Mock.Of<IBackgroundJobQueue>();

        // Follower не использует Heartbeat - это только для Leader
        var heartbeatTimer = Mock.Of<ITimer>();

        // Очередь команд - абстракция только для синхронного выполнения команд (потом может вынесу выше)
        var commandQueue = new SimpleCommandQueue();

        // Нужно только лидеру
        var requestQueueFactory = Mock.Of<IRequestQueueFactory>();


        var electionTimer = Mock.Of<ITimer>();
        var commandSerializer =
            Mock.Of<ICommandSerializer<int>>(x => x.Serialize(It.IsAny<int>()) == Array.Empty<byte>());

        var (persistenceFacade, fileSystem) = CreateStorage();

        var node = new RaftConsensusModule(NodeId, peers, Logger.None, electionTimer, heartbeatTimer,
            backgroundJobQueue,
            persistenceFacade,
            commandQueue,
            Helpers.NullStateMachine,
            commandSerializer,
            requestQueueFactory,
            Helpers.NullStateMachineFactory);

        node.SetStateTest(new FollowerState<int, int>(node, node.StateMachineFactory, commandSerializer, Logger.None));

        return new NodeCreateResult(node, persistenceFacade, fileSystem);

        (StoragePersistenceFacade, ConsensusFileSystem) CreateStorage()
        {
            var (_, log, metadata, snapshot, tempDir) = Helpers.CreateFileSystem();
            var logFile = new FileLogStorage(log.Open(FileMode.OpenOrCreate));
            var metadataFile = new FileMetadataStorage(metadata.Open(FileMode.OpenOrCreate), new Term(1), null);
            var snapshotFile = new FileSystemSnapshotStorage(snapshot, tempDir);
            return ( new StoragePersistenceFacade(logFile, metadataFile, snapshotFile),
                     new ConsensusFileSystem(log, metadata, snapshot) );
        }
    }

    [Fact]
    public void InstallSnapshot__ДолженСоздатьНовыйФайлСнапшота()
    {
        var (node, persistence, fs) = CreateFollowerNodeNew();
        fs.SnapshotFile.Delete();

        var lastIncludedIndex = 10;
        var lastIncludedTerm = new Term(2);
        var snapshotData = new byte[] {1, 2, 3};
        var request = new InstallSnapshotRequest(new Term(2), new NodeId(1), lastIncludedIndex, lastIncludedTerm,
            new StubSnapshot(snapshotData));
        node.Handle(request);


        var (index, term, data) = persistence.ReadSnapshotFileTest();
        Assert.Equal(lastIncludedIndex, index);
        Assert.Equal(lastIncludedTerm, term);
        Assert.Equal(snapshotData, data);
    }

    [Fact]
    public void InstallSnapshot__СуществующийПустойФайлСнапшотаДолженПерезаписаться()
    {
        var (node, persistence, _) = CreateFollowerNodeNew();

        var lastIncludedIndex = 10;
        var lastIncludedTerm = new Term(2);
        var snapshotData = new byte[] {1, 2, 3};
        var request = new InstallSnapshotRequest(new Term(2), new NodeId(1), lastIncludedIndex, lastIncludedTerm,
            new StubSnapshot(snapshotData));
        node.Handle(request);

        var (index, term, data) = persistence.ReadSnapshotFileTest();
        Assert.Equal(lastIncludedIndex, index);
        Assert.Equal(lastIncludedTerm, term);
        Assert.Equal(snapshotData, data);
    }

    [Fact]
    public void InstallSnapshot__СуществующийФайлСнапшотаДолженПерезаписаться()
    {
        var (node, persistence, _) = CreateFollowerNodeNew();
        persistence.SnapshotStorage.WriteSnapshotDataTest(new Term(123), 876,
            new StubSnapshot(new byte[] {9, 5, 234, 1, 6, 2, 44, 2, 7, 45, 52, 97}));

        var lastIncludedIndex = 10;
        var lastIncludedTerm = new Term(2);
        var snapshotData = new byte[] {1, 2, 3};
        var request = new InstallSnapshotRequest(new Term(2), new NodeId(1), lastIncludedIndex, lastIncludedTerm,
            new StubSnapshot(snapshotData));
        node.Handle(request);

        var (index, term, data) = persistence.ReadSnapshotFileTest();
        Assert.Equal(lastIncludedIndex, index);
        Assert.Equal(lastIncludedTerm, term);
        Assert.Equal(snapshotData, data);
    }

    private static readonly NodeId AnotherNodeId = new NodeId(NodeId.Id + 1);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void AppendEntries__КогдаЛогПустойБезЗакомиченныхЗаписей__ДолженДобавитьЗаписиВЛог(int entriesCount)
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(term, new[] {( byte ) i}))
                                .ToArray();

        // Лог изначально был пуст и у нас, и у лидера
        // Причем, еще ничего не закомичено
        var node = CreateFollowerNode(term, null);

        var request = new AppendEntriesRequest(term, LogEntryInfo.TombIndex, AnotherNodeId, LogEntryInfo.Tomb, entries);

        var response = node.Handle(request);

        Assert.True(response.Success);
        var actualEntries = node.PersistenceFacade.ReadLogFull();
        Assert.Equal(entries, actualEntries, LogEntryEqualityComparer.Instance);
    }

    [Fact]
    public void AppendEntries__КогдаЕстьКонфликтующиеЗаписи__ДолженПерезатеретьНеЗакомиченные()
    {
        var term = new Term(2);
        var node = CreateFollowerNode(term, null);

        // 3 закоммиченные записи с индексами
        var committedEntries = new LogEntry[]
        {
            new(new(1), new byte[] {1}), new(new(2), new byte[] {2}), new(new(2), new byte[] {3}),
        };
        node.PersistenceFacade.LogStorage.AppendRange(committedEntries);

        // Добавляем 2 незакоммиченные записи
        node.PersistenceFacade.InsertRange(new LogEntry[] {new(new(3), new byte[] {4}), new(new(3), new byte[] {5}),},
            3);

        // В запросе передается 1 запись (идет сразу после наших закоммиченных) 
        var leaderCommit = 2;
        var newEntries = new LogEntry[] {new(new(3), new byte[] {6}), new(new(3), new byte[] {7}),};
        var prevLogEntryInfo = new LogEntryInfo(committedEntries[^1].Term, committedEntries.Length - 1);
        var request = new AppendEntriesRequest(term, leaderCommit, AnotherNodeId, prevLogEntryInfo, newEntries);

        var response = node.Handle(request);

        Assert.True(response.Success);
        var actualEntries = node.PersistenceFacade.ReadLogFull();
        Assert.Equal(committedEntries.Concat(newEntries), actualEntries, LogEntryEqualityComparer.Instance);
        Assert.Equal(request.Term, response.Term);
        Assert.Equal(node.CurrentTerm, response.Term);
    }
}