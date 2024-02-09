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
public class CandidateStateTests
{
    private static readonly NodeId NodeId = new(1);
    private static readonly PeerGroup EmptyPeerGroup = new(Array.Empty<IPeer>());

    private static readonly IDeltaExtractor<int> NullDeltaExtractor =
        new Mock<IDeltaExtractor<int>>()
           .Apply(m =>
            {
                var delta = new byte[] {1};
                m.Setup(x => x.TryGetDelta(It.IsAny<int>(), out delta))
                 .Returns(true);
            })
           .Object;


    private Mock<IPersistence> _mockPersistence;

    private RaftConsensusModule CreateCandidateNode(Term term,
                                                    ITimer? electionTimer = null,
                                                    IBackgroundJobQueue? jobQueue = null,
                                                    IEnumerable<IPeer>? peers = null,
                                                    IApplicationFactory? applicationFactory = null,
                                                    Action<Mock<IPersistence>>? persistenceFactory = null)
    {
        var mp = new Mock<IPersistence>(MockBehavior.Strict)
           .Apply(m =>
            {
                m.SetupGet(p => p.CurrentTerm).Returns(term);

                // Выставляем это дополнительно, т.к. при инициализации выставляется в обработчиках узлов
                m.SetupGet(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
                persistenceFactory?.Invoke(m);
            });
        _mockPersistence = mp;
        electionTimer ??= Mock.Of<ITimer>();
        var timerFactory = electionTimer is null
                               ? Helpers.NullTimerFactory
                               : new ConstantTimerFactory(electionTimer);
        jobQueue ??= Mock.Of<IBackgroundJobQueue>();
        var peerGroup = peers != null
                            ? new PeerGroup(peers.ToArray())
                            : EmptyPeerGroup;
        var node = new RaftConsensusModule(NodeId, peerGroup,
            Logger.None, timerFactory, jobQueue,
            mp.Object,
            NullDeltaExtractor, applicationFactory ?? Helpers.NullApplicationFactory);
        node.SetStateTest(node.CreateCandidateState());
        return node;
    }

    private const int DefaultTerm = 1;

    [Fact]
    public void ElectionTimout__КогдаЕдинственныйВКластере__ДолженПерейтиВСледующийТермИСтатьЛидером()
    {
        var electionTimer = new Mock<ITimer>();
        electionTimer.SetupAdd(x => x.Timeout += null);
        var currentTerm = new Term(1);
        var expectedTerm = currentTerm.Increment();
        var node = CreateCandidateNode(currentTerm, electionTimer.Object, persistenceFactory: m =>
        {
            m.Setup(p => p.CommitIndex).Returns(Lsn.Tomb);
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == expectedTerm), It.Is<NodeId?>(id => id == NodeId)))
             .Verifiable();
            ISnapshot s = null;
            var lei = LogEntryInfo.Tomb;
            m.Setup(p => p.TryGetSnapshot(out s, out lei)).Returns(false);
            m.Setup(p => p.ReadCommittedDeltaFromPreviousSnapshot()).Returns(Array.Empty<byte[]>());
        });

        electionTimer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
        _mockPersistence.Verify(
            p => p.UpdateState(It.Is<Term>(t => t == expectedTerm), It.Is<NodeId?>(id => id == NodeId)), Times.Once());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void ElectionTimeout__КогдаНиктоНеОтдалГолос__ДолженПерейтиВСледующийТермИОстатьсяКандидатом(int peersCount)
    {
        var electionTimer = new Mock<ITimer>();
        electionTimer.SetupAdd(x => x.Timeout += null);
        var currentTerm = new Term(1);
        var expectedTerm = currentTerm.Increment();
        var stubJobQueue = Helpers.NullBackgroundJobQueue;
        var peers = Enumerable.Range(0, peersCount)
                              .Select(_ => new StubQuorumPeer(currentTerm, false))
                              .ToArray();
        using var node = CreateCandidateNode(DefaultTerm, electionTimer.Object, jobQueue: stubJobQueue, peers: peers,
            persistenceFactory:
            m =>
            {
                m.Setup(p => p.CommitIndex).Returns(Lsn.Tomb);
                m.Setup(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
                ISnapshot s = null!;
                var slei = LogEntryInfo.Tomb;
                m.Setup(p => p.TryGetSnapshot(out s, out slei)).Returns(false);
                m.Setup(p => p.ReadCommittedDeltaFromPreviousSnapshot()).Returns(Array.Empty<byte[]>());
                m.Setup(p => p.UpdateState(It.Is<Term>(t => t == expectedTerm), It.Is<NodeId?>(id => id == NodeId)))
                 .Verifiable();
            });

        electionTimer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Candidate, node.CurrentRole);

        _mockPersistence.Verify(
            p => p.UpdateState(It.Is<Term>(t => t == expectedTerm), It.Is<NodeId?>(id => id == NodeId)), Times.Once());
    }

    [Fact]
    public void ElectionTimeout__КогдаДругихУзловНет__ДолженСтатьЛидеромПоТаймауту()
    {
        var oldTerm = new Term(1);
        var nextTerm = oldTerm.Increment();
        var timer = new Mock<ITimer>().Apply(m =>
        {
            m.SetupAdd(t => t.Timeout += null);
        });
        using var node = CreateCandidateNode(oldTerm.Value, electionTimer: timer.Object, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == nextTerm), It.Is<NodeId?>(id => id == NodeId)))
             .Verifiable();
            ISnapshot s = null;
            var lei = LogEntryInfo.Tomb;
            m.Setup(p => p.TryGetSnapshot(out s, out lei)).Returns(false);
            m.Setup(p => p.ReadCommittedDeltaFromPreviousSnapshot()).Returns(Array.Empty<byte[]>());
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        timer.Raise(t => t.Timeout += null);

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
        _mockPersistence.Verify(p => p.UpdateState(It.Is<Term>(t => t == nextTerm), It.Is<NodeId?>(id => id == NodeId)),
            Times.Once());
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
        private readonly RequestVoteResponse _response;
        public NodeId Id => AnotherNodeId;

        public StubQuorumPeer(RequestVoteResponse response)
        {
            _response = response;
        }

        public StubQuorumPeer(Term term, bool voteGranted)
        {
            _response = new RequestVoteResponse(term, voteGranted);
        }

        public AppendEntriesResponse SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
        {
            throw new Exception("Кандидат не должен отсылать AppendEntries");
        }

        public RequestVoteResponse SendRequestVote(RequestVoteRequest request, CancellationToken token)
        {
            return _response;
        }

        public InstallSnapshotResponse SendInstallSnapshot(
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
        var term = new Term(2);
        var peers = Enumerable.Range(0, grantedVotesCount)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .Concat(Enumerable.Range(0, nonGrantedVotesCount)
                                                .Select(_ => new StubQuorumPeer(term, false)));

        var queue = new AwaitingTaskBackgroundJobQueue();
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: peers, persistenceFactory: m =>
        {
            ISnapshot x = null!;
            var lei = LogEntryInfo.Tomb;
            m.Setup(p => p.TryGetSnapshot(out x, out lei)).Returns(false);
            m.Setup(p => p.ReadCommittedDeltaFromPreviousSnapshot()).Returns(Array.Empty<byte[]>());
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        queue.RunWait();

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    [Theory(Timeout = 1000 * 2)]
    [InlineData(0, 1)]
    [InlineData(0, 2)]
    [InlineData(1, 2)]
    [InlineData(1, 3)]
    [InlineData(2, 3)]
    [InlineData(4, 5)]
    [InlineData(1, 4)]
    [InlineData(2, 4)]
    [InlineData(3, 4)]
    public async Task Кворум__СНесколькимиУзлами__ДолженОстатьсяКандидатомЕслиНеСобралКворум(
        int grantedVotesCount,
        int nonGrantedVotesCount)
    {
        var term = new Term(2);
        var peers = Enumerable.Range(0, grantedVotesCount)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .Concat(Enumerable.Range(0, nonGrantedVotesCount)
                                                .Select(_ => new StubQuorumPeer(term, false)));

        var queue = new AwaitingTaskBackgroundJobQueue();
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: peers);

        queue.RunWait();

        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
    }

    [Fact]
    public void Кворум__КогдаУзелОтветилБольшимТермомИНеОтдалГолос__ДолженСтатьFollower()
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();
        var newTerm = term.Increment();
        var peer = new StubQuorumPeer(new RequestVoteResponse(newTerm, false));
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: new[] {peer}, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(newTerm, null));
        });

        queue.Run();

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void Кворум__КогдаУзелОтветилБольшимТермомИНеОтдалГолос__ДолженОбновитьТерм()
    {
        var term = new Term(1);
        var queue = new SingleRunBackgroundJobQueue();
        var newTerm = term.Increment();
        var peer = new StubQuorumPeer(new RequestVoteResponse(newTerm, false));
        using var node = CreateCandidateNode(term, jobQueue: queue, peers: new[] {peer}, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(newTerm, It.IsAny<NodeId?>())).Verifiable();
            m.Setup(p => p.LastEntry).Returns(LogEntryInfo.Tomb);
        });

        queue.Run();

        _mockPersistence.Verify(p => p.UpdateState(newTerm, It.IsAny<NodeId?>()));
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеТекущего__ДолженСтатьFollower()
    {
        var term = new Term(2);
        var lastLogEntryInfo = LogEntryInfo.Tomb;
        var newTerm = term.Increment();
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.CurrentTerm).Returns(term);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
        });
        var request = new RequestVoteRequest(AnotherNodeId, newTerm, lastLogEntryInfo);
        _ = node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермБольшеТекущего__ДолженОбновитьСвойТерм()
    {
        var term = new Term(2);
        var newTerm = term.Increment();
        var lastLogEntryInfo = LogEntryInfo.Tomb;
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.CurrentTerm).Returns(term);
            m.Setup(p => p.VotedFor).Returns(( NodeId? ) null);
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == newTerm), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
        });

        var response = node.Handle(new RequestVoteRequest(AnotherNodeId, newTerm, lastLogEntryInfo));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.VoteGranted);
        Assert.Equal(newTerm, response.CurrentTerm);
    }

    [Fact]
    public void RequestVote__КогдаТермБолееВысокийИЛогАктуальныйИНеГолосовали__ДолженОтдатьГолосЗаЭтотУзел()
    {
        var term = new Term(2);
        var newTerm = term.Increment();
        var lastLogEntryInfo = LogEntryInfo.Tomb;
        var candidateId = AnotherNodeId;
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.CurrentTerm).Returns(term);
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == candidateId))).Verifiable();
            m.Setup(p => p.VotedFor).Returns(( NodeId? ) null);
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, newTerm, lastLogEntryInfo));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.VoteGranted);
        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == candidateId)),
            Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермРавенИЛогАктуальныйИОтдавалиГолосЗаЭтотГолос__ДолженОтдатьГолосЗаЭтотУзел()
    {
        var term = new Term(2);
        var newTerm = term.Increment();
        var lastLogEntryInfo = LogEntryInfo.Tomb;
        var candidateId = AnotherNodeId;
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.CurrentTerm).Returns(term);
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(true);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == candidateId))).Verifiable();
            m.Setup(p => p.VotedFor).Returns(candidateId);
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, newTerm, lastLogEntryInfo));

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
        Assert.True(response.VoteGranted);
        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == candidateId)),
            Times.Once());
    }

    private static LogEntry RandomDataEntry(Term term)
    {
        var data = new byte[Random.Shared.Next(0, 128)];
        Random.Shared.NextBytes(data);
        return new LogEntry(term, data);
    }

    [Fact]
    public void AppendEntries__КогдаТермЗапросаРавенТермуУзлаИЛогНеКонфликтует__ДолженСтатьПоследователем()
    {
        var term = new Term(2);
        var prevEntryInfo = new LogEntryInfo(term, 123);
        var requestEntries = Enumerable.Range(0, 10)
                                       .Select(_ => RandomDataEntry(term))
                                       .ToArray();

        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevEntryInfo))).Returns(true);
            m.Setup(p =>
                p.InsertRange(It.Is<IReadOnlyList<LogEntry>>(l =>
                        l.SequenceEqual(requestEntries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(l => l == prevEntryInfo.Index + 1)));
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.SetupGet(p => p.LastEntry).Returns(prevEntryInfo);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
            m.Setup(p => p.Commit(It.IsAny<Lsn>()));
        });

        var response =
            node.Handle(new AppendEntriesRequest(term, Lsn.Tomb, AnotherNodeId, prevEntryInfo, requestEntries));

        Assert.True(response.Success);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void AppendEntries__КогдаТермЗапросаБольшеТермаУзлаИЛогНеКонфликтует__ДолженСтатьПоследователем()
    {
        var term = new Term(2);
        var candidateTerm = term.Increment();
        var prevEntryInfo = new LogEntryInfo(term, 123);
        var requestEntries = Enumerable.Range(0, 10)
                                       .Select(_ => RandomDataEntry(term))
                                       .ToArray();

        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevEntryInfo))).Returns(true);
            m.Setup(p =>
                p.InsertRange(It.Is<IReadOnlyList<LogEntry>>(l =>
                        l.SequenceEqual(requestEntries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(l => l == prevEntryInfo.Index + 1)));
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.SetupGet(p => p.LastEntry).Returns(prevEntryInfo);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
            m.Setup(p => p.Commit(It.IsAny<Lsn>()));
        });

        var response =
            node.Handle(new AppendEntriesRequest(candidateTerm, Lsn.Tomb, AnotherNodeId, prevEntryInfo,
                requestEntries));

        Assert.True(response.Success);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void AppendEntries__КогдаТермЗапросаБольшеТермаУзлаИЛогНеКонфликтует__ДолженВыставитьГолосВNull()
    {
        var term = new Term(2);
        var candidateTerm = term.Increment();
        var prevEntryInfo = new LogEntryInfo(term, 123);
        var requestEntries = Enumerable.Range(0, 10)
                                       .Select(_ => RandomDataEntry(term))
                                       .ToArray();

        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevEntryInfo))).Returns(true);
            m.Setup(p =>
                p.InsertRange(It.Is<IReadOnlyList<LogEntry>>(l =>
                        l.SequenceEqual(requestEntries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(l => l == prevEntryInfo.Index + 1)));
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.Is<NodeId?>(id => id == null)));
            m.Setup(p => p.LastEntry).Returns(prevEntryInfo);
            m.Setup(p => p.CommitIndex).Returns(Lsn.Tomb);
            m.Setup(p => p.Commit(It.IsAny<Lsn>()));
        });

        var response =
            node.Handle(new AppendEntriesRequest(candidateTerm, Lsn.Tomb, AnotherNodeId, prevEntryInfo,
                requestEntries));

        Assert.True(response.Success);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void AppendEntries__КогдаТермЗапросаБольшеТермаУзлаИЛогНеКонфликтует__ДолженОбновитьТерм()
    {
        var term = new Term(2);
        var candidateTerm = term.Increment();
        var prevEntryInfo = new LogEntryInfo(term, 123);
        var requestEntries = Enumerable.Range(0, 10)
                                       .Select(_ => RandomDataEntry(term))
                                       .ToArray();

        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevEntryInfo))).Returns(true);
            m.Setup(p =>
                p.InsertRange(It.Is<IReadOnlyList<LogEntry>>(l =>
                        l.SequenceEqual(requestEntries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(l => l == prevEntryInfo.Index + 1)));
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == candidateTerm), It.IsAny<NodeId?>())).Verifiable();
            m.SetupGet(p => p.LastEntry).Returns(prevEntryInfo);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
            m.Setup(p => p.Commit(It.IsAny<Lsn>()));
        });

        _ = node.Handle(new AppendEntriesRequest(candidateTerm, Lsn.Tomb, AnotherNodeId, prevEntryInfo,
            requestEntries));

        _mockPersistence.Verify(p => p.UpdateState(It.Is<Term>(t => t == candidateTerm), It.IsAny<NodeId?>()),
            Times.Once());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(1000)]
    public void AppendEntries__КогдаПрефиксСовпадаетИТермНеМеньше__ДолженВставитьЗаписи(long candidateTermDelta)
    {
        var term = new Term(2);
        var requestEntries = Enumerable.Range(0, 10)
                                       .Select(_ => RandomDataEntry(term))
                                       .ToArray();
        var prevEntryInfo = new LogEntryInfo(2, 1000);
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevEntryInfo))).Returns(true);
            m.Setup(p =>
                p.InsertRange(It.Is<IReadOnlyList<LogEntry>>(l =>
                        l.SequenceEqual(requestEntries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(lsn => lsn == prevEntryInfo.Index + 1)));
            m.Setup(p => p.Commit(It.IsAny<Lsn>()));
            m.Setup(p => p.LastEntry).Returns(prevEntryInfo);
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
        });

        var response = node.Handle(new AppendEntriesRequest(term.Value + candidateTermDelta, Lsn.Tomb, AnotherNodeId,
            prevEntryInfo, requestEntries));

        Assert.True(response.Success);
    }

    [Theory]
    [InlineData(-1, 0)]
    [InlineData(0, 1)]
    [InlineData(0, 2)]
    [InlineData(1, 2)]
    [InlineData(9, 20)]
    [InlineData(1233, 1500)]
    public void AppendEntries__КогдаИндексКоммитаВЗапросеБольшеМоего__ДолженЗакоммититьЗаписи(
        int currentCommitIndex,
        int requestCommitIndex)
    {
        var term = new Term(2);
        var logEntries = Enumerable.Range(0, requestCommitIndex + 1)
                                   .Select(_ => RandomDataEntry(term))
                                   .ToArray();
        var prevLogEntryInfo = new LogEntryInfo(term, 213);
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.SetupGet(p => p.LastEntry).Returns(new LogEntryInfo(term, requestCommitIndex));
            m.Setup(p => p.CommitIndex).Returns(currentCommitIndex);
            m.Setup(p => p.Commit(It.Is<Lsn>(lsn => lsn == requestCommitIndex)));
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevLogEntryInfo))).Returns(true);
            m.Setup(p =>
                p.InsertRange(It.Is<IReadOnlyList<LogEntry>>(list =>
                        list.SequenceEqual(logEntries, new LogEntryEqualityComparer())),
                    It.Is<Lsn>(lsn => lsn == prevLogEntryInfo.Index + 1)));
            m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
        });

        var response = node.Handle(new AppendEntriesRequest(term, requestCommitIndex, AnotherNodeId, prevLogEntryInfo,
            Array.Empty<LogEntry>()));

        Assert.True(response.Success);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(0, 5)]
    [InlineData(5, 2)]
    public void
        AppendEntries__КогдаКогдаИндексКоммитаВЗапросеБольшеИндексаПоследнейЗаписи__ДолженЗакоммититьТолькоСвоюПоследнююЗапись(
        int requestCommitIndex,
        int myLastIndex)
    {
        var term = new Term(2);
        var requestEntries = Array.Empty<LogEntry>();
        var lastEntry = new LogEntryInfo(term, myLastIndex);
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.Commit(It.Is<Lsn>(lsn => lsn == myLastIndex)));
            m.SetupGet(p => p.CommitIndex).Returns(requestCommitIndex - 1);
            m.SetupGet(p => p.LastEntry).Returns(lastEntry);
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()));
            m.Setup(p => p.Commit(It.IsAny<Lsn>()));
            m.Setup(p => p.ShouldCreateSnapshot()).Returns(false);
        });

        var response = node.Handle(new AppendEntriesRequest(term, requestCommitIndex, AnotherNodeId,
            node.Persistence.LastEntry, requestEntries));

        Assert.True(response.Success);
    }

    [Fact]
    public void AppendEntries__КогдаЛогКонфликтует__ДолженОтветитьFalse()
    {
        var term = new Term(5);
        var prevLogEntry = new LogEntryInfo(term, 53);
        var enqueueEntries = new[] {new LogEntry(term, "asdfasdf"u8.ToArray())};
        using var node = CreateCandidateNode(term, persistenceFactory: m =>
        {
            m.Setup(p => p.InsertRange(It.IsAny<IReadOnlyList<LogEntry>>(), It.IsAny<Lsn>()))
             .Throws(new InvalidOperationException("Не должен быть вызван"));
            m.Setup(p => p.PrefixMatch(It.Is<LogEntryInfo>(lei => lei == prevLogEntry))).Returns(false);
            m.SetupGet(p => p.CurrentTerm).Returns(term);
        });

        var response = node.Handle(new AppendEntriesRequest(term, 0, AnotherNodeId, prevLogEntry, enqueueEntries));

        Assert.False(response.Success);
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеНоЛогКонфликтует__ДолженПерейтиВНовыйТерм()
    {
        var currentTerm = new Term(2);
        var lastLogEntryInfo = new LogEntryInfo(new Term(1), 1);
        var newTerm = currentTerm.Increment();
        using var node = CreateCandidateNode(currentTerm, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(false);
            m.Setup(p => p.UpdateState(It.Is<Term>(t => t == newTerm), It.IsAny<NodeId?>())).Verifiable();
        });

        _ = node.Handle(new RequestVoteRequest(AnotherNodeId, newTerm, lastLogEntryInfo));

        _mockPersistence.Verify(p => p.UpdateState(It.Is<Term>(t => t == newTerm), It.IsAny<NodeId?>()), Times.Once());
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеНоЛогКонфликтует__ДолженВернутьFalse()
    {
        var currentTerm = new Term(2);
        var lastLogEntryInfo = new LogEntryInfo(new Term(1), 1);
        var newTerm = currentTerm.Increment();
        using var node = CreateCandidateNode(currentTerm, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(false);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
        });

        var response = node.Handle(new RequestVoteRequest(AnotherNodeId, newTerm, lastLogEntryInfo));

        Assert.False(response.VoteGranted);
    }

    [Fact]
    public void RequestVote__КогдаТермВЗапросеБольшеНоЛогКонфликтует__ДолженВыставитьГолосВnull()
    {
        var currentTerm = new Term(2);
        var lastLogEntryInfo = new LogEntryInfo(new Term(1), 1);
        var newTerm = currentTerm.Increment();
        var candidateId = AnotherNodeId;
        using var node = CreateCandidateNode(currentTerm, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.Is<LogEntryInfo>(lei => lei == lastLogEntryInfo))).Returns(false);
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), null)).Verifiable();
            m.SetupGet(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        var response = node.Handle(new RequestVoteRequest(candidateId, newTerm, lastLogEntryInfo));

        Assert.False(response.VoteGranted);
        _mockPersistence.Verify(p => p.UpdateState(It.IsAny<Term>(), null), Times.Once());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void AppendEntries__КогдаПришелОтАктуальногоЛидера__НеДолженСтатьНовымЛидеромПослеОкончанияКворума(
        int peersCount)
    {
        /*
         * Когда во время сбора кворма мне приходит AppendEntries (или другой запрос),
         * то собранный кворум не должен изменять уже новое состояние
         */
        var currentTerm = new Term(2);
        var queue = new SingleRunBackgroundJobQueue();
        var requestTerm = currentTerm.Increment();
        var stubPeers = Enumerable.Range(0, peersCount).Select(_ => new StubQuorumPeer(currentTerm, true)).ToArray();
        using var node = CreateCandidateNode(currentTerm, jobQueue: queue, peers: stubPeers, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(true);
            m.Setup(p => p.PrefixMatch(It.IsAny<LogEntryInfo>())).Returns(true);
            m.Setup(p => p.CommitIndex).Returns(Lsn.Tomb);
        });

        var response = node.Handle(new AppendEntriesRequest(requestTerm, Lsn.Tomb, AnotherNodeId, LogEntryInfo.Tomb,
            Array.Empty<LogEntry>()));
        queue.Run();

        Assert.True(response.Success);
        Assert.Equal(requestTerm, response.Term);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void RequestVote__КогдаТермыОдинаковы__ДолженВернутьFalse()
    {
        var currentTerm = new Term(2);
        var queue = new SingleRunBackgroundJobQueue();
        using var node = CreateCandidateNode(currentTerm, jobQueue: queue, persistenceFactory: m =>
        {
            m.Setup(p => p.IsUpToDate(It.IsAny<LogEntryInfo>())).Returns(true);
        });

        var response = node.Handle(new RequestVoteRequest(AnotherNodeId, currentTerm, LogEntryInfo.Tomb));

        Assert.False(response.VoteGranted);
        Assert.Equal(currentTerm, response.CurrentTerm);
        Assert.Equal(NodeRole.Candidate, node.CurrentRole);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public void RequestVote__КогдаТермБольше__НеДолженСтатьЛидеромДажеПриПолученииВсехГолосовВКворуме(int peersCount)
    {
        /*
         * Когда во время сбора кворма мне приходит AppendEntries (или другой запрос),
         * то собранный кворум не должен изменять уже новое состояние
         */
        var currentTerm = new Term(2);
        var queue = new SingleRunBackgroundJobQueue();
        var requestTerm = currentTerm.Increment();
        var lastEntryInfo = new LogEntryInfo(requestTerm, 123);
        var stubPeers = Enumerable.Range(0, peersCount).Select(_ => new StubQuorumPeer(currentTerm, true)).ToArray();
        using var node = CreateCandidateNode(currentTerm, jobQueue: queue, peers: stubPeers, persistenceFactory: m =>
        {
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), It.IsAny<NodeId?>()));
            m.Setup(p => p.VotedFor).Returns(NodeId);
            m.Setup(p => p.IsUpToDate(lastEntryInfo)).Returns(true);
        });

        var response = node.Handle(new RequestVoteRequest(AnotherNodeId, requestTerm, lastEntryInfo));
        queue.Run();

        Assert.True(response.VoteGranted);
        Assert.Equal(requestTerm, response.CurrentTerm);
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    private static void SetupStubLeaderInitialization(Mock<IPersistence> mock)
    {
        mock.Setup(p => p.CommitIndex).Returns(Lsn.Tomb);
        ISnapshot s = null!;
        var lei = LogEntryInfo.Tomb;
        mock.Setup(p => p.TryGetSnapshot(out s, out lei)).Returns(false);
        mock.Setup(p => p.ReadCommittedDeltaFromPreviousSnapshot()).Returns(Array.Empty<byte[]>());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    public void ПослеСтановленияЛидером__КогдаКворумСобран__ДолженОстановитьElectionТаймер(int votes)
    {
        var term = new Term(1);
        var queue = new AwaitingTaskBackgroundJobQueue();

        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new RequestVoteResponse(CurrentTerm: term, VoteGranted: true));

        var electionTimer = new Mock<ITimer>().Apply(m =>
        {
            m.Setup(x => x.Stop())
             .Verifiable();
        });

        var peers = Enumerable.Range(0, votes)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .ToArray();

        using var _ = CreateCandidateNode(term,
            electionTimer: electionTimer.Object,
            jobQueue: queue,
            peers: peers,
            persistenceFactory: m =>
            {
                SetupStubLeaderInitialization(m);
            });

        queue.RunWait();

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
    public void Кворум__КогдаБольшинствоГолосовОтдано__ДолженПерейтиВLeader(
        int successResponses,
        int notResponded)
    {
        var term = new Term(2);
        var peers = Enumerable.Range(0, successResponses)
                              .Select(_ => new StubQuorumPeer(term, true))
                              .Concat(Enumerable.Range(0, notResponded)
                                                .Select(_ => new StubQuorumPeer(new Term(term.Value - 1), false)))
                              .ToArray();
        var queue = new AwaitingTaskBackgroundJobQueue();

        using var node = CreateCandidateNode(term, jobQueue: queue, peers: peers, persistenceFactory: m =>
        {
            m.Setup(p => p.CommitIndex).Returns(Lsn.Tomb);
            ISnapshot s = null!;
            var lei = LogEntryInfo.Tomb;
            m.Setup(p => p.TryGetSnapshot(out s, out lei)).Returns(false);
            m.Setup(p => p.ReadCommittedDeltaFromPreviousSnapshot()).Returns(Array.Empty<byte[]>());
            m.Setup(p => p.UpdateState(It.IsAny<Term>(), null));
        });
        queue.RunWait();

        Assert.Equal(NodeRole.Leader, node.CurrentRole);
    }

    /// <summary>
    /// Реализация фоновой очереди задач, которая при запуске прекращает регистрацию новых.
    /// Нужно для того, чтобы после становления лидером не регистрировать новые,
    /// иначе при запуске (<see cref="RunWait"/>) добавляются новые (переход в лидера) и вызывается исключение.
    /// </summary>
    private class AwaitingTaskBackgroundJobQueue : IBackgroundJobQueue
    {
        private readonly List<(IBackgroundJob Job, CancellationToken Token)> _list = new();
        private bool _sealed;

        public void Accept(IBackgroundJob job, CancellationToken token)
        {
            if (!_sealed)
            {
                _list.Add(( job, token ));
            }
        }

        public void RunWait()
        {
            _sealed = true;
            try
            {
                Task.WaitAll(_list.Select(tuple => Task.Run(() => tuple.Job.Run(tuple.Token), tuple.Token)).ToArray());
            }
            catch (AggregateException e) when (e.InnerException is OperationCanceledException)
            {
            }
        }
    }
}