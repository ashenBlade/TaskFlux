using Moq;
using Serilog.Core;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Tests.Infrastructure;
using TaskFlux.Consensus.Tests.Stubs;
using TaskFlux.Core;

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Raft")]
public class FollowerStateTests
{
    private static readonly NodeId NodeId = new(1);

    private static readonly PeerGroup EmptyPeerGroup = new(Array.Empty<IPeer>());

    private Mock<IPersistence> _mockPersistence;

    private RaftConsensusModule<int, int> CreateFollowerNode(Term currentTerm,
                                                             NodeId? votedFor,
                                                             ITimer? electionTimer = null,
                                                             IBackgroundJobQueue? jobQueue = null,
                                                             Action<Mock<IPersistence>>? persistenceFactory = null)
    {
        var timerFactory = electionTimer is not null
                               ? new ConstantTimerFactory(electionTimer)
                               : Helpers.NullTimerFactory;
        var persistence = new Mock<IPersistence>(MockBehavior.Strict)
           .Apply(m =>
            {
                m.SetupGet(p => p.CurrentTerm).Returns(currentTerm);
                m.SetupGet(p => p.VotedFor).Returns(votedFor);
                persistenceFactory?.Invoke(m);
            });
        _mockPersistence = persistence;
        var node = new RaftConsensusModule(NodeId,
            EmptyPeerGroup,
            Logger.None,
            timerFactory,
            jobQueue ?? Helpers.NullBackgroundJobQueue,
            persistence.Object,
            Helpers.NullDeltaExtractor,
            Helpers.NullApplicationFactory);

        node.SetStateTest(node.CreateFollowerState());

        return node;
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогАктуальный__ДолженВыставитьVotedForВIdКандидата()
    {
        var oldTerm = new Term(1);
        var candidateTerm = oldTerm.Increment();
        var lastLogEntryInfo = LogEntryInfo.Tomb;
        var candidateId = AnotherNodeId;
        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.VotedFor).Returns(( NodeId? ) null);
            m.Setup(p => p.IsUpToDate(lastLogEntryInfo)).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), candidateId)).Verifiable();
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, candidateTerm, lastLogEntryInfo));

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), candidateId), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеИЛогАктуальный__ДолженОбновитьТерм()
    {
        var oldTerm = new Term(1);
        var candidateTerm = oldTerm.Increment();
        var lastLogEntryInfo = LogEntryInfo.Tomb;
        var candidateId = AnotherNodeId;
        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.VotedFor).Returns(( NodeId? ) null);
            m.Setup(p => p.IsUpToDate(lastLogEntryInfo)).Returns(true);
            m.Setup(p => p.UpdateState(candidateTerm, It.IsAny<NodeId?>())).Verifiable();
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, candidateTerm, lastLogEntryInfo));

        _mockPersistence.Verify(p => p.UpdateState(candidateTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольше__ДолженВыставитьТермКакВЗапросе()
    {
        var oldTerm = new Term(1);
        var lastLogEntryInfo = new LogEntryInfo(oldTerm, 0);
        var expectedTerm = oldTerm.Increment();
        var candidateId = new NodeId(2);
        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.VotedFor).Returns(( NodeId? ) null);
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == expectedTerm), It.IsAny<NodeId?>()));
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, expectedTerm, lastLogEntryInfo));

        Assert.True(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогАктуальный__ДолженВыставитьОтданныйГолосВКандидата()
    {
        var oldTerm = new Term(1);
        var expectedTerm = oldTerm.Increment();
        var candidateId = new NodeId(2);
        var lastLogEntryInfo = new LogEntryInfo(oldTerm, 0);

        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), candidateId)).Verifiable();
            m.SetupGet(p => p.VotedFor).Returns(( NodeId? ) null);
        });

        var request = new RequestVoteRequest(candidateId, expectedTerm, lastLogEntryInfo);

        _ = node.Handle(request);

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), candidateId), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеИЛогНеАктуальный__ДолженВыставитьОтданныйГолосВnull()
    {
        var oldTerm = new Term(1);
        var expectedTerm = oldTerm.Increment();
        var candidateId = new NodeId(2);
        var lastLogEntryInfo = new LogEntryInfo(oldTerm, 0);

        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(false);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), null)).Verifiable();
            m.SetupGet(p => p.VotedFor).Returns(( NodeId? ) null);
        });

        var request = new RequestVoteRequest(candidateId, expectedTerm, lastLogEntryInfo);

        _ = node.Handle(request);

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), null), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаЛогАктуаленИТермКандидатаБольшеИРанееНеГолосовал__ДолженОтдатьГолос()
    {
        var term = new Term(1);
        var candidateId = AnotherNodeId;
        var candidateTerm = term.Increment();
        var candidateLastEntry = new LogEntryInfo(candidateTerm, 100);
        using var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == candidateLastEntry))).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == candidateId)));
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, candidateTerm, candidateLastEntry));

        Assert.True(response.VoteGranted);
        Assert.Equal(candidateTerm, response.CurrentTerm);
    }

    [Fact]
    public void RequestVote__КогдаЛогАктуаленИТермРавенИРанееНеГолосовал__ДолженОтветитьTrue()
    {
        var term = new Term(1);
        var candidateId = new NodeId(2);
        var lastLogEntryInfo = new LogEntryInfo(term, 0);
        using var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(lastLogEntryInfo)).Returns(true);
            m.Setup(p => p.UpdateState(term, candidateId));
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, term, lastLogEntryInfo));

        Assert.True(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаЛогАктуаленИТермРавенИРанееНеГолосовал__ДолженВыставитьVotedForВIdКандидата()
    {
        var term = new Term(1);
        var candidateId = new NodeId(2);
        var lastLogEntryInfo = new LogEntryInfo(term, 0);
        using var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(lastLogEntryInfo)).Returns(true);
            m.Setup(p => p.UpdateState(term, candidateId)).Verifiable();
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, term, lastLogEntryInfo));

        _mockPersistence.Verify(p => p.UpdateState(term, candidateId), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаЛогАктуаленИТермРавенИРанееГолосовалЗаЭтотУзел__ДолженВернутьTrue()
    {
        var term = new Term(1);
        var candidateId = new NodeId(2);
        var lastLogEntryInfo = new LogEntryInfo(term, 0);
        using var node = CreateFollowerNode(term, candidateId, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(lastLogEntryInfo)).Returns(true);
            m.Setup(p => p.UpdateState(term, candidateId)).Verifiable();
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, term, lastLogEntryInfo));

        Assert.True(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаЛогАктуаленИТермРавенИРанееГолосовалЗаДругойУзел__ДолженВернутьFalse()
    {
        var term = new Term(1);
        var candidateId = new NodeId(2);
        var anotherNodeId = new NodeId(3);
        var lastLogEntryInfo = new LogEntryInfo(term, 0);
        using var node = CreateFollowerNode(term, anotherNodeId, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(lastLogEntryInfo)).Returns(true);
            m.Setup(p => p.UpdateState(term, candidateId));
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, term, lastLogEntryInfo));

        Assert.False(response.VoteGranted);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(3, 2)]
    [InlineData(3, 1)]
    public void RequestVote__КогдаТермВЗапросеМеньше__ДолженОтветитьFalse(
        int myTerm,
        int otherTerm)
    {
        var oldTerm = new Term(myTerm);
        var lesserTerm = new Term(otherTerm);
        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
        });

        var response = node.Handle(new RequestVoteRequest(AnotherNodeId, lesserTerm, new LogEntryInfo(oldTerm, 0)));

        Assert.False(response.VoteGranted);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(3, 2)]
    [InlineData(3, 1)]
    public void RequestVote__КогдаТермВЗапросеМеньше__ДолженВернутьСвойТерм(
        int myTerm,
        int otherTerm)
    {
        var currentTerm = new Term(myTerm);
        var lesserTerm = new Term(otherTerm);
        using var node = CreateFollowerNode(currentTerm, null, persistenceFactory: _ =>
        {
        });

        var response = node.Handle(new RequestVoteRequest(AnotherNodeId, lesserTerm, new LogEntryInfo(currentTerm, 0)));

        Assert.Equal(currentTerm, response.CurrentTerm);
    }

    [Fact]
    public void ElectionTimeout__ДолженПерейтиВСостояниеCandidate()
    {
        var timer = new Mock<ITimer>(MockBehavior.Loose);

        using var node = CreateFollowerNode(new Term(1), null, electionTimer: timer.Object, persistenceFactory: m =>
        {
            m.SetupGet(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
        });

        timer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
    }

    [Fact]
    public void ElectionTimeout__ДолженПерейтиВСледующийТерм()
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        var expectedTerm = oldTerm.Increment();
        using var raft = CreateFollowerNode(new Term(1), null, timer.Object, persistenceFactory: m =>
        {
            m.SetupGet(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
            m.Setup(p => p.UpdateState(expectedTerm, It.IsAny<NodeId?>())).Verifiable();
        });

        timer.Raise(x => x.Timeout += null);

        _mockPersistence.Verify(p => p.UpdateState(expectedTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void ElectionTimeout__ДолженВыставитьVotedForВСвойId()
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var node = CreateFollowerNode(oldTerm, null, timer.Object, persistenceFactory: m =>
        {
            m.SetupGet(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), NodeId)).Verifiable();
        });

        timer.Raise(x => x.Timeout += null);

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), NodeId), Times.Once());
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

        var prevLogEntryInfo = new LogEntryInfo(term, 0);
        var leaderCommit = Lsn.Tomb;
        using var node = CreateFollowerNode(term, null, electionTimer: timer.Object, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(prevLogEntryInfo)).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(leaderCommit);
        });

        var request =
            new AppendEntriesRequest(term, leaderCommit, AnotherNodeId, prevLogEntryInfo, Array.Empty<LogEntry>());

        node.Handle(request);

        timer.Verify(x => x.Stop(), Times.Once());
        // Считаем еще самый первый запуск
        timer.Verify(x => x.Schedule(), Times.Exactly(2));
    }

    [Fact]
    public void AppendEntries__КогдаПереданБольшийТерм__ДолженВыставитьVotedForВNull()
    {
        var oldTerm = new Term(1);
        var nextTerm = new Term(oldTerm.Value + 123);
        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), null)).Verifiable();
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        node.Handle(new AppendEntriesRequest(nextTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), null), Times.Once());
    }

    [Fact]
    public void AppendEntries__КогдаПереданБольшийТерм__ДолженОстатьсяFollower()
    {
        var oldTerm = new Term(1);
        var nextTerm = new Term(oldTerm.Value + 66);

        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        node.Handle(AppendEntriesRequest.Heartbeat(nextTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void AppendEntries__КогдаПереданБольшийТерм__ДолженОбновитьТерм()
    {
        var oldTerm = new Term(5);
        var nextTerm = oldTerm.Increment();

        using var node = CreateFollowerNode(oldTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(nextTerm, It.IsAny<NodeId?>())).Verifiable();
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        _ = node.Handle(AppendEntriesRequest.Heartbeat(nextTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb));

        _mockPersistence.Verify(p => p.UpdateState(nextTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void InstallSnapshot__КогдаУстановленУспешно__ДолженВызыватьCommitПослеУстановки()
    {
        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var snapshotData = new byte[] {1, 2, 3};
        var currentTerm = 1;

        // Приходится использовать Loose, т.к. нельзя матчить с Span
        var msi = new MockSnapshotInstaller();

        using var node = CreateFollowerNode(currentTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.CreateSnapshot(It.Is<LogEntryInfo>(lei => lei == lastIncludedEntry)))
             .Returns(msi);
        });

        _ = node.Handle(new InstallSnapshotRequest(currentTerm, AnotherNodeId, lastIncludedEntry,
            new StubSnapshot(snapshotData)));

        Assert.True(msi.CommitCalled);
    }

    [Fact]
    public void InstallSnapshot__КогдаПереданныйТермБольше__ДолженОбновитьТерм()
    {
        var currentTerm = new Term(1);
        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var snapshotData = new byte[] {1, 2, 3};
        var leaderTerm = currentTerm.Increment();

        // Приходится использовать Loose, т.к. нельзя матчить с Span
        var msi = new MockSnapshotInstaller();

        var node = CreateFollowerNode(currentTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.CreateSnapshot(lastIncludedEntry))
             .Returns(msi);
            m.Setup(p => p.UpdateState(leaderTerm, It.IsAny<NodeId?>())).Verifiable();
        });

        var response = node.Handle(new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot(snapshotData)));

        Assert.Equal(leaderTerm, response.CurrentTerm);
        _mockPersistence.Verify(p => p.UpdateState(leaderTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    private class MockSnapshotInstaller : ISnapshotInstaller
    {
        public bool CommitCalled { get; set; }
        public bool DiscardCalled { get; set; }
        public List<byte> InstalledChunks { get; } = new();

        public void InstallChunk(ReadOnlySpan<byte> chunk, CancellationToken token)
        {
            InstalledChunks.AddRange(chunk);
        }

        public void Commit()
        {
            CommitCalled = true;
        }

        public void Discard()
        {
            DiscardCalled = true;
        }
    }

    [Fact]
    public void InstallSnapshot__КогдаВозниклаОшибка__ДолженВызыватьDiscard()
    {
        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);


        var mockSnapshot = new Mock<ISnapshot>().Apply(m =>
        {
            IEnumerable<ReadOnlyMemory<byte>> StubChunkData()
            {
                yield return new byte[] {1, 1, 1};
                throw new EndOfStreamException("Соединение представим разорвалось и это исключение было выкинуто");
            }

            m.Setup(x => x.GetAllChunks(It.IsAny<CancellationToken>())).Returns(StubChunkData);
        });

        var currentTerm = new Term(1);
        var msi = new MockSnapshotInstaller();
        using var node = CreateFollowerNode(currentTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.CreateSnapshot(It.Is<LogEntryInfo>(lei => lei == lastIncludedEntry)))
             .Returns(msi);
        });

        Assert.Throws<EndOfStreamException>(() =>
            node.Handle(new InstallSnapshotRequest(currentTerm, AnotherNodeId, lastIncludedEntry,
                mockSnapshot.Object)));
        Assert.True(msi.DiscardCalled);
    }

    [Fact]
    public void InstallSnapshot__КогдаТермЛидераМеньше__ДолженВернутьСвойТермВОтвете()
    {
        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var leaderTerm = new Term(2);

        var nodeTerm = 3;
        using var node = CreateFollowerNode(nodeTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.CreateSnapshot(It.Is<LogEntryInfo>(lei => lei == lastIncludedEntry)))
             .Throws(new InvalidOperationException("Нельзя даже вызывать этот метод"));
        });

        var response = node.Handle(new InstallSnapshotRequest(leaderTerm, new NodeId(1), lastIncludedEntry,
            new StubSnapshot([1, 2, 4, 8, 16, 32])));

        Assert.Equal(nodeTerm, response.CurrentTerm);
    }

    [Fact]
    public void InstallSnapshot__КогдаТермЛидераМеньше__НеДолженСоздаватьНовыйСнапшот()
    {
        var lastIncludedEntry = new LogEntryInfo(new Term(2), 10);
        var leaderTerm = new Term(2);

        var nodeTerm = 3;
        var node = CreateFollowerNode(nodeTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.CreateSnapshot(It.Is<LogEntryInfo>(lei => lei == lastIncludedEntry)))
             .Throws(new InvalidOperationException("Нельзя даже вызывать этот метод"));
        });

        var ex = Record.Exception(() => node.Handle(new InstallSnapshotRequest(leaderTerm, new NodeId(1),
            lastIncludedEntry, new StubSnapshot([1, 2, 4, 8, 16, 32]))));

        Assert.IsNotType<InvalidOperationException>(ex);
    }

    private static readonly NodeId AnotherNodeId = new(NodeId.Id + 1);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void AppendEntries__КогдаПрефиксСовпадает__ДолженВставитьЗаписи(int entriesCount)
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(term, [( byte ) i]))
                                .ToArray();

        var prevLsn = 30;
        var prevLogEntryInfo = new LogEntryInfo(term, prevLsn);
        var commitIndex = Lsn.Tomb;
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevLogEntryInfo)))
             .Returns(true);
            m.Setup(p =>
                p.InsertRange(
                    It.Is<IReadOnlyList<LogEntry>>(x => x.SequenceEqual(entries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(l => l == prevLsn + 1)));
            m.SetupGet(p => p.CommitIndex).Returns(commitIndex);
        });

        var request = new AppendEntriesRequest(term, commitIndex, AnotherNodeId, prevLogEntryInfo, entries);

        var response = node.Handle(request);

        Assert.True(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаПрефиксСовпадает__ДолженВернутьTrue()
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, 10)
                                .Select(i => new LogEntry(term, [( byte ) i]))
                                .ToArray();

        var prevLsn = 30;
        var prevLogEntryInfo = new LogEntryInfo(term, prevLsn);
        var commitIndex = Lsn.Tomb;
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevLogEntryInfo)))
             .Returns(true);
            m.Setup(p => p.InsertRange(
                It.Is<IReadOnlyList<LogEntry>>(x => x.SequenceEqual(entries, new LogEntryEqualityComparer())),
                prevLsn + 1));
            m.SetupGet(p => p.CommitIndex).Returns(commitIndex);
        });

        var response =
            node.Handle(new AppendEntriesRequest(term, commitIndex, AnotherNodeId, prevLogEntryInfo, entries));

        Assert.True(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаТермБольше__ДолженВыставитьТермКакВЗапросе()
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, 10)
                                .Select(i => new LogEntry(term, [( byte ) i]))
                                .ToArray();

        var prevLsn = 30;
        var prevLogEntryInfo = new LogEntryInfo(term, prevLsn);
        var commitIndex = Lsn.Tomb;
        var requestTerm = term.Increment();
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevLogEntryInfo)))
             .Returns(true);
            m.Setup(p => p.InsertRange(
                It.Is<IReadOnlyList<LogEntry>>(x => x.SequenceEqual(entries, new LogEntryEqualityComparer())),
                prevLsn + 1));
            m.Setup(p => p.UpdateState(requestTerm, It.IsAny<NodeId?>())).Verifiable();
            m.SetupGet(p => p.CommitIndex).Returns(commitIndex);
        });

        _ = node.Handle(new AppendEntriesRequest(requestTerm, commitIndex, AnotherNodeId, prevLogEntryInfo, entries));

        _mockPersistence.Verify(p => p.UpdateState(requestTerm, It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void AppendEntries__КогдаТермБольше__ДолженВыставитьVotedForВNull()
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, 10)
                                .Select(i => new LogEntry(term, [( byte ) i]))
                                .ToArray();

        var prevLsn = 30;
        var prevLogEntryInfo = new LogEntryInfo(term, prevLsn);
        var commitIndex = Lsn.Tomb;
        var requestTerm = term.Increment();
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevLogEntryInfo)))
             .Returns(true);
            m.Setup(p => p.InsertRange(
                It.Is<IReadOnlyList<LogEntry>>(x => x.SequenceEqual(entries, new LogEntryEqualityComparer())),
                prevLsn + 1));
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), null)).Verifiable();
            m.SetupGet(p => p.CommitIndex).Returns(commitIndex);
        });

        _ = node.Handle(new AppendEntriesRequest(requestTerm, commitIndex, AnotherNodeId, prevLogEntryInfo, entries));

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), null), Times.Once());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public void AppendEntries__КогдаПрефиксСовпадаетИИндексКоммитаБольше__ДолженЗакоммититьЗаписи(int entriesCount)
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(term, [( byte ) i]))
                                .ToArray();

        var prevLsn = 30;
        var prevLogEntryInfo = new LogEntryInfo(term, prevLsn);

        var nodeCommitIndex = Lsn.Tomb;
        var requestCommitIndex = nodeCommitIndex + 1;
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevLogEntryInfo)))
             .Returns(true);
            m.Setup(p =>
                p.InsertRange(
                    It.Is<IReadOnlyList<LogEntry>>(x => x.SequenceEqual(entries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(l => l == prevLsn + 1)));
            m.SetupGet(p => p.CommitIndex).Returns(nodeCommitIndex);
            m.Setup(p => p.Commit(It.Is<Lsn>(l => l == requestCommitIndex)));
            m.Setup(p => p.LastEntry).Returns(prevLogEntryInfo with {Index = requestCommitIndex});
            m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
        });

        var request = new AppendEntriesRequest(term, requestCommitIndex, AnotherNodeId, prevLogEntryInfo, entries);

        var response = node.Handle(request);

        Assert.True(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаПрефиксНеСовпадает__ДолженВернутьFalse()
    {
        var term = new Term(2);
        var entries = Enumerable.Range(1, 10)
                                .Select(i => new LogEntry(term, new[] {( byte ) i}))
                                .ToArray();

        var requestPrevEntry = new LogEntryInfo(term.Increment(), entries.Length - 1);
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == requestPrevEntry))).Returns(false);
            m.SetupGet(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
        });

        var response = node.Handle(new AppendEntriesRequest(term, Lsn.Tomb, AnotherNodeId, requestPrevEntry, entries));

        Assert.False(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаИндексКоммитаБольшеИндексаПоследнейЗаписи__ДолженЗакоммититьТолькоПоследнююЗапись()
    {
        var term = new Term(2);
        var entriesCount = 10;
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(term, new[] {( byte ) i}))
                                .ToArray();

        var requestPrevEntry = new LogEntryInfo(term.Increment(), entries.Length - 1);
        var nodeLastEntry = new LogEntryInfo(term, 30);
        var requestCommitIndex = nodeLastEntry.Index + entriesCount + 5;
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == requestPrevEntry))).Returns(true);
            m.Setup(p =>
                p.InsertRange(
                    It.Is<IReadOnlyList<LogEntry>>(l => l.SequenceEqual(entries, new LogEntryEqualityComparer())),
                    requestPrevEntry.Index + 1));
            m.SetupGet(p => p.LastEntry).Returns(nodeLastEntry);
            m.Setup(p => p.Commit(nodeLastEntry.Index)).Verifiable();
            m.SetupGet(p => p.CommitIndex).Returns(nodeLastEntry.Index - 5);
            m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
        });

        var response =
            node.Handle(new AppendEntriesRequest(term, requestCommitIndex, AnotherNodeId, requestPrevEntry, entries));

        Assert.True(response.Success);
        _mockPersistence.Verify(p => p.Commit(nodeLastEntry.Index), Times.Once());
    }

    [Fact]
    public void AppendEntries__КогдаПрефиксНеСовпадает__НеДолженВставлятьЗаписи()
    {
        var term = new Term(2);
        var entriesCount = 10;
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(term, new[] {( byte ) i}))
                                .ToArray();

        var requestPrevEntry = new LogEntryInfo(term.Increment(), entries.Length - 1);
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(requestPrevEntry)).Returns(false);
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
        });

        var response = node.Handle(new AppendEntriesRequest(term, Lsn.Tomb, AnotherNodeId, requestPrevEntry, entries));

        Assert.False(response.Success);
        _mockPersistence.Verify(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()),
            Times.Never());
    }

    [Fact]
    public void AppendEntries__КогдаПрефиксНеСовпадает__НеДолженКоммититьЗаписи()
    {
        var term = new Term(2);
        var entriesCount = 10;
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(term, new[] {( byte ) i}))
                                .ToArray();

        var requestPrevEntry = new LogEntryInfo(term.Increment(), entries.Length - 1);
        var node = CreateFollowerNode(term, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(requestPrevEntry)).Returns(false);
            m.Setup(p => p.Commit(It.IsAny<Lsn>())).Verifiable();
        });

        var response = node.Handle(new AppendEntriesRequest(term, Lsn.Tomb, AnotherNodeId, requestPrevEntry, entries));

        Assert.False(response.Success);
        _mockPersistence.Verify(p => p.Commit(It.IsAny<Lsn>()), Times.Never());
    }

    [Fact]
    public void AppendEntries__КогдаТермМеньше__ДолженВернутьFalse()
    {
        var currentTerm = new Term(10);
        var lesserTerm = new Term(1);
        var entriesCount = 10;
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(currentTerm, [( byte ) i]))
                                .ToArray();

        var requestPrevEntry = new LogEntryInfo(currentTerm.Increment(), entries.Length - 1);
        var node = CreateFollowerNode(currentTerm, null, persistenceFactory: _ =>
        {
        });

        var response =
            node.Handle(new AppendEntriesRequest(lesserTerm, Lsn.Tomb, AnotherNodeId, requestPrevEntry, entries));

        Assert.False(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаЕстьЗаписи__ДолженВставитьИхПоСледующемуИндексуПослеПереданного()
    {
        var currentTerm = new Term(10);
        var entriesCount = 10;
        var lastLogEntry = new LogEntryInfo(currentTerm, 5);
        var prevLogEntry = new LogEntryInfo(currentTerm, 3); // Представим, что нужно затереть 
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(currentTerm, [( byte ) i]))
                                .ToArray();

        var node = CreateFollowerNode(currentTerm, null, persistenceFactory: m =>
        {
            m.Setup(p =>
                  p.InsertRange(
                      It.Is<IReadOnlyList<LogEntry>>(list =>
                          list.SequenceEqual(entries, new LogEntryEqualityComparer())),
                      prevLogEntry.Index + 1))
             .Verifiable();
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
            m.SetupGet(p => p.LastEntry).Returns(lastLogEntry);
        });

        _ = node.Handle(new AppendEntriesRequest(currentTerm, Lsn.Tomb, AnotherNodeId, prevLogEntry, entries));

        _mockPersistence.Verify(p =>
            p.InsertRange(
                It.Is<IReadOnlyList<LogEntry>>(list => list.SequenceEqual(entries, new LogEntryEqualityComparer())),
                prevLogEntry.Index + 1), Times.Once());
    }

    [Fact]
    public void AppendEntries__КогдаИндексКоммитаВЗапросеБольше__ДолженЗакоммититьПоУказанномуИндексу()
    {
        var currentTerm = new Term(10);
        var entriesCount = 10;
        var oldCommitIndex = 12;
        var newCommitIndex = 15;
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(currentTerm, [( byte ) i]))
                                .ToArray();
        var requestPrevEntry = new LogEntryInfo(currentTerm.Increment(), entries.Length - 1);

        var node = CreateFollowerNode(currentTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.SetupGet(p => p.CommitIndex).Returns(oldCommitIndex);
            m.Setup(p => p.Commit(newCommitIndex)).Verifiable();
            m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(currentTerm, newCommitIndex + 1));
            m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
        });

        _ = node.Handle(new AppendEntriesRequest(currentTerm, newCommitIndex, AnotherNodeId, requestPrevEntry,
            entries));

        _mockPersistence.Verify(p => p.Commit(newCommitIndex), Times.Once());
    }

    [Fact]
    public void
        AppendEntries__КогдаИндексКоммитаВЗапросеБольшеИндексаПоследнейЗаписиВЛоге__ДолженЗакоммититьИндексПоследнейЗаписи()
    {
        var currentTerm = new Term(10);
        var entriesCount = 10;
        var oldCommitIndex = 12;
        var lastEntryIndex = 14;
        var newCommitIndex = 20;
        var entries = Enumerable.Range(1, entriesCount)
                                .Select(i => new LogEntry(currentTerm, [( byte ) i]))
                                .ToArray();

        var requestPrevEntry = new LogEntryInfo(currentTerm.Increment(), entries.Length - 1);
        var node = CreateFollowerNode(currentTerm, null, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.SetupGet(p => p.CommitIndex).Returns(oldCommitIndex);
            m.Setup(p => p.Commit(lastEntryIndex)).Verifiable();
            m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(currentTerm, lastEntryIndex));
            m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
        });

        _ = node.Handle(new AppendEntriesRequest(currentTerm, newCommitIndex, AnotherNodeId, requestPrevEntry,
            entries));

        _mockPersistence.Verify(p => p.Commit(lastEntryIndex), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаУжеГолосовалВТермеИУзелНеТотЗаКоторогоГолосовали__НеДолженОтдатьГолос()
    {
        var term = new Term(4);
        var candidateId = new NodeId(2);
        var votedFor = new NodeId(3);

        var lastLogEntryInfo = new LogEntryInfo(new Term(2), 1);
        var node = CreateFollowerNode(term, votedFor, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.SetupGet(p => p.VotedFor).Returns(votedFor);
            m.SetupGet(p => p.CurrentTerm).Returns(term);
        });

        var request = new RequestVoteRequest(candidateId, term, lastLogEntryInfo);

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаУжеОтдавалГолосЗаУзелВЭтомТермеИЛогАктуальныйИТермРавен__ДолженВернутьTrue()
    {
        var term = new Term(4);
        var candidateId = new NodeId(5);
        var lastLogEntryInfo = new LogEntryInfo(new Term(2), 1);

        using var node = CreateFollowerNode(term, candidateId, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.VotedFor).Returns(candidateId);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, term, lastLogEntryInfo));

        Assert.True(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаУжеОтдавалГолосЗаУзелВЭтомТермеИЛогАктуальныйИТермРавен__НеДолженОбновлятьСостояние()
    {
        var term = new Term(4);
        var candidateId = new NodeId(5);
        var lastLogEntryInfo = new LogEntryInfo(new Term(2), 1);

        using var node = CreateFollowerNode(term, candidateId, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.VotedFor).Returns(candidateId);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>())).Verifiable();
        });

        _ = node.Handle(new RequestVoteRequest(candidateId, term, lastLogEntryInfo));

        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()), Times.Never());
    }
}