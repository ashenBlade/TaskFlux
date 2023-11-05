using System.IO.Abstractions;
using Castle.Core;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
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
using IApplication = Consensus.Raft.Tests.Infrastructure.IApplication;

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
        var timerFactory = electionTimer != null
                               ? new ConstantTimerFactory(electionTimer)
                               : Helpers.NullTimerFactory;
        var backgroundJobQueue = jobQueue ?? Helpers.NullBackgroundJobQueue;

        var persistence = new StoragePersistenceFacade(new FileLogStorage(fs.Log, fs.TemporaryDirectory),
            new FileMetadataStorage(fs.Metadata.Open(FileMode.OpenOrCreate), currentTerm, votedFor),
            new FileSystemSnapshotStorage(fs.Snapshot, fs.TemporaryDirectory, Logger.None));

        var node = new RaftConsensusModule(NodeId,
            EmptyPeerGroup,
            Logger.None,
            timerFactory,
            backgroundJobQueue,
            persistence,
            Helpers.NullApplication,
            Helpers.NullCommandSerializer,
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
        node.PersistenceFacade.InsertBufferRange(new LogEntry[]
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
        node.PersistenceFacade.InsertBufferRange(new LogEntry[]
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

        node.PersistenceFacade.InsertBufferRange(
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


    private record ConsensusFileSystem(IFileInfo SnapshotFile);

    private record NodeCreateResult(RaftConsensusModule Module,
                                    StoragePersistenceFacade Persistence,
                                    ConsensusFileSystem FileSystem);

    private static NodeCreateResult CreateFollowerNodeNew(IApplicationFactory? factory = null)
    {
        // Follower никому ничего не отправляет, только принимает
        var peers = new PeerGroup(Array.Empty<IPeer>());

        // Follower не использует фоновые задачи - это только для Candidate/Leader
        var backgroundJobQueue = Mock.Of<IBackgroundJobQueue>();

        var commandSerializer =
            Mock.Of<ICommandSerializer<int>>(x => x.Serialize(It.IsAny<int>()) == Array.Empty<byte>());

        var (persistenceFacade, fileSystem) = CreateStorage();
        factory ??= Helpers.NullApplicationFactory;
        var node = new RaftConsensusModule(NodeId,
            peers, Logger.None,
            Helpers.NullTimerFactory,
            backgroundJobQueue,
            persistenceFacade,
            Helpers.NullApplication,
            commandSerializer,
            factory);

        node.SetStateTest(node.CreateFollowerState());

        return new NodeCreateResult(node, persistenceFacade, fileSystem);

        (StoragePersistenceFacade, ConsensusFileSystem) CreateStorage()
        {
            var (_, log, metadata, snapshot, tempDir) = Helpers.CreateFileSystem();
            var logFile = new FileLogStorage(log, tempDir);
            var metadataFile = new FileMetadataStorage(metadata.Open(FileMode.OpenOrCreate), new Term(1), null);
            var snapshotFile = new FileSystemSnapshotStorage(snapshot, tempDir, Logger.None);
            return ( new StoragePersistenceFacade(logFile, metadataFile, snapshotFile),
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
        foreach (var response in node.Handle(request))
        {
            response.CurrentTerm
                    .Should()
                    .Be(leaderTerm, "отправленный лидером терм больше текущего");
        }


        var (index, term, data) = persistence.ReadSnapshotFileTest();
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

        foreach (var response in node.Handle(request))
        {
            Assert.Equal(leaderTerm, response.CurrentTerm);
        }

        var (index, term, data) = persistence.ReadSnapshotFileTest();
        Assert.Equal(lastIncludedEntry.Index, index);
        Assert.Equal(lastIncludedEntry.Term, term);
        Assert.Equal(snapshotData, data);
    }

    [Fact]
    public void InstallSnapshot__СуществующийФайлСнапшотаДолженПерезаписаться()
    {
        var (node, persistence, _) = CreateFollowerNodeNew();
        persistence.SnapshotStorage.WriteSnapshotDataTest(new Term(2), 3,
            new StubSnapshot(new byte[] {9, 5, 234, 1, 6, 2, 44, 2, 7, 45, 52, 97}));

        var lastIncludedEntry = new LogEntryInfo(new Term(4), 13);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(4);
        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));

        foreach (var response in node.Handle(request))
        {
            response.CurrentTerm
                    .Should()
                    .Be(leaderTerm, "терм лидера больше текущего");
        }

        var (index, term, data) = persistence.ReadSnapshotFileTest();
        Assert.Equal(lastIncludedEntry.Index, index);
        Assert.Equal(lastIncludedEntry.Term, term);
        Assert.Equal(snapshotData, data);
        Assert.Equal(lastIncludedEntry, persistence.SnapshotStorage.LastLogEntry);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void InstallSnapshot__КогдаЧанковНесколько__ДолженВернутьОтветовНа2БольшеЧемЧанков(int chunksCount)
    {
        var (node, _, _) = CreateFollowerNodeNew();

        var snapshot = new Mock<ISnapshot>();
        snapshot.Setup(x => x.GetAllChunks(It.IsAny<CancellationToken>()))
                .Returns(GetAllChunks());

        var leaderTerm = new Term(10);
        var lastLogEntry = new LogEntryInfo(new Term(9), 10);


        var responses = node
                       .Handle(new InstallSnapshotRequest(leaderTerm, AnotherNodeId, lastLogEntry, snapshot.Object))
                       .ToList();

        responses
           .Should()
           .AllSatisfy(r =>
            {
                r.CurrentTerm.Should().Be(leaderTerm, "терм лидера больше");
            })
           .And
           .HaveCount(chunksCount + 2,
                $"{chunksCount} ответов для чанков, 1 после валидации заголовка и еще 1 последний, подтверждающий");

        return;

        IEnumerable<ReadOnlyMemory<byte>> GetAllChunks()
        {
            for (int i = 0; i < chunksCount; i++)
            {
                var buffer = new byte[Random.Shared.Next(1, 100)];
                Random.Shared.NextBytes(buffer);
                yield return buffer;
            }
        }
    }

    [Fact]
    public void InstallSnapshot__КогдаЧанкДанныхОдин__ДолженВернуть3Ответа()
    {
        var (node, persistence, _) = CreateFollowerNodeNew();

        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(4);
        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));

        var responses = node.Handle(request)
                            .ToList();

        responses
           .Should()
           .AllSatisfy(x =>
            {
                x.CurrentTerm
                 .Should()
                 .Be(leaderTerm, "терм должен быть как у лидера");
            }, "терм во время работы не меняется")
           .And
           .HaveCount(3, "первый ответ");

        var (index, term, data) = persistence.ReadSnapshotFileTest();
        Assert.Equal(lastIncludedEntry.Index, index);
        Assert.Equal(lastIncludedEntry.Term, term);
        Assert.Equal(snapshotData, data);
    }

    [Fact]
    public void InstallSnapshot__КогдаТермЛидераМеньше__ДолженВернуть1Ответ()
    {
        var leaderTerm = new Term(4);
        var nodeTerm = leaderTerm.Increment();

        var (node, persistence, _) = CreateFollowerNodeNew();
        persistence.UpdateState(nodeTerm, null);

        var snapshotData = new byte[] {1, 2, 3};
        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));

        var responses = node.Handle(request)
                            .ToList();

        responses
           .Should()
           .AllSatisfy(x =>
            {
                x.CurrentTerm.Should().Be(nodeTerm, "терм должен быть как у лидера");
            }, "терм во время работы не меняется")
           .And
           .HaveCount(1, "только один ответ, что терм узла больше терм лидера");
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
        var actualEntries = node.PersistenceFacade.ReadLogFullTest();
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
        node.PersistenceFacade.InsertBufferRange(
            new LogEntry[] {new(new(3), new byte[] {4}), new(new(3), new byte[] {5}),},
            3);

        // В запросе передается 1 запись (идет сразу после наших закоммиченных) 
        var leaderCommit = 2;
        var newEntries = new LogEntry[] {new(new(3), new byte[] {6}), new(new(3), new byte[] {7}),};
        var prevLogEntryInfo = new LogEntryInfo(committedEntries[^1].Term, committedEntries.Length - 1);
        var request = new AppendEntriesRequest(term, leaderCommit, AnotherNodeId, prevLogEntryInfo, newEntries);

        var response = node.Handle(request);

        Assert.True(response.Success);
        var actualEntries = node.PersistenceFacade.ReadLogFullTest();
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
        node.PersistenceFacade.InsertBufferRange(new LogEntry[]
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
        node.PersistenceFacade.InsertBufferRange(new LogEntry[]
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
    public void InstallSnapshot__ДолженВыставитьНовоеСостояние()
    {
        var application = new StubApplication(123);
        var applicationFactory = new Mock<IApplicationFactory>().Apply(f =>
        {
            f.Setup(x => x.Restore(It.IsAny<ISnapshot>()))
             .Returns(application);
            f.Setup(x => x.CreateEmpty())
             .Throws(new InvalidOperationException(
                  "Приложение должно восстановиться из снапшота, а не создаваться заново"));
        });

        var (node, _, fs) = CreateFollowerNodeNew(factory: applicationFactory.Object);
        fs.SnapshotFile.Delete();

        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(2);

        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));
        foreach (var response in node.Handle(request))
        {
            response.CurrentTerm
                    .Should()
                    .Be(leaderTerm, "отправленный лидером терм больше текущего");
        }

        node.Application
            .Should()
            .Be(application, comparer: ReferenceEqualityComparer<StubApplication>.Instance,
                 becauseArgs: "нужно восстановить состояние из снапшота");
    }

    private class StubApplication : IApplication
    {
        public int Value { get; }

        public StubApplication(int value)
        {
            Value = value;
        }

        public int Apply(int command)
        {
            return Value;
        }

        public void ApplyNoResponse(int command)
        {
        }

        public ISnapshot GetSnapshot()
        {
            return new StubSnapshot(Array.Empty<byte>());
        }
    }

    [Fact]
    public void InstallSnapshot__ДолженОчиститьЛогИБуфер()
    {
        var (node, persistence, fs) = CreateFollowerNodeNew();
        fs.SnapshotFile.Delete();

        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = new Term(2);
        var request = new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData));

        foreach (var response in node.Handle(request))
        {
            response.CurrentTerm
                    .Should()
                    .Be(leaderTerm, "отправленный лидером терм больше текущего");
        }

        persistence.ReadLogBufferTest()
                   .Should()
                   .BeEmpty("лог должен быть очищен после установки нового снапшота");

        persistence.ReadLogFileTest()
                   .Should()
                   .BeEmpty("лог должен быть очищен после установки нового снапшота");
    }

    [Fact]
    public void InstallSnapshot__КогдаИндексСнапшотаРавенТекущемуИндексу__ДолженУстановитьСнапшот()
    {
        // Такое может случиться, когда на лидера большая нагрузка идет и отправляется тот же самый снапшот

        var (follower, storage, _) = CreateFollowerNodeNew(Helpers.NullApplicationFactory);
        var snapshot = new StubSnapshot(new byte[] {1, 2, 3});
        var term = new Term(2);
        var logEntryInfo = new LogEntryInfo(new Term(2), 1000);
        var oldSnapshot = new StubSnapshot(new byte[] {4, 5, 6});
        storage.SnapshotStorage.WriteSnapshotDataTest(logEntryInfo.Term, logEntryInfo.Index, oldSnapshot);

        var request = new InstallSnapshotRequest(term, AnotherNodeId, logEntryInfo, snapshot);

        foreach (var _ in follower.Handle(request))
        {
        }

        var (lastIndex, lastTerm, snapshotData) = storage.SnapshotStorage.ReadAllDataTest();
        lastIndex.Should()
                 .Be(logEntryInfo.Index, "индекс записи в снапшоте должна быть такая же как и в запросе");
        lastTerm.Should()
                .Be(logEntryInfo.Term, "терм записи в снапшоте должна быть такой же как и в запросе");
        snapshotData
           .Should()
           .Equal(snapshot.Data, "данные снапшота должны быть перезаписаны");
    }
}