using System.Runtime.CompilerServices;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using Consensus.Raft.State.LeaderState;
using Consensus.Raft.Tests.Infrastructure;
using Consensus.Raft.Tests.Stubs;
using Moq;
using TaskFlux.Core;

namespace Consensus.Raft.Tests;

[Trait("Category", "Raft")]
public class LeaderStateTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task HeartbeatTimer__ДолженОтправитьHeartbeatНаДругиеУзлы(int peersCount)
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });

        var jobQueue = new SingleRunBackgroundJobQueue();

        var peers = Enumerable.Range(0, peersCount)
                              .Select(_ => new Mock<IPeer>()
                                  .Apply(p => p.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                                                    It.IsAny<CancellationToken>()))
                                               .ReturnsAsync(new AppendEntriesResponse(term, true))
                                               .Verifiable()))
                              .ToList();

        var log = new Mock<IPersistenceFacade>().Apply(l =>
        {
            l.Setup(x => x.GetFrom(It.IsAny<int>())).Returns(Array.Empty<LogEntry>());
        });

        using var node = CreateLeaderNode(term,
            null,
            peers.Select(x => x.Object),
            heartbeatTimer: heartbeatTimer.Object,
            jobQueue: jobQueue,
            log: log.Object,
            requestQueueFactory: new SingleHeartbeatRequestQueueFactory(0));

        heartbeatTimer.Raise(x => x.Timeout += null);

        jobQueue.Run();

        peers.ForEach(p =>
            p.Verify(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()),
                Times.Once()));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void ПриСрабатыванииHeartbeatTimer__ДолженЗапуститьHeartbeatТаймерЗаново(int peersCount)
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });

        var peers = Enumerable.Range(0, peersCount)
                              .Select(_ => new Mock<IPeer>()
                                  .Apply(p => p.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                                                    It.IsAny<CancellationToken>()))
                                               .ReturnsAsync(new AppendEntriesResponse(term, true))
                                               .Verifiable()))
                              .ToList();
        using var stateMachine =
            CreateLeaderNode(term, null, peers.Select(x => x.Object), heartbeatTimer: heartbeatTimer.Object);

        heartbeatTimer.Raise(x => x.Timeout += null);

        heartbeatTimer.Verify(x => x.Start(), Times.Once());
    }

    private static readonly NodeId NodeId = new NodeId(1);
    private static readonly NodeId AnotherNodeId = new NodeId(NodeId.Id + 1);

    [Fact]
    public void ПриОбрабаткеЗапросаRequestVote__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var term = new Term(1);

        using var node = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(CandidateId: AnotherNodeId, CandidateTerm: term.Increment(),
            LastLogEntryInfo: node.PersistenceFacade.LastEntry);

        node.Handle(request);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }

    [Fact]
    public void
        ПриОбрабаткеЗапросаRequestVote__СБолееВысокимТермом__ПослеПереходаВFollowerДолженСброситьHeartbeatТаймер()
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Start());
            t.Setup(x => x.Stop()).Verifiable();
        });

        using var node = CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object);

        var request = new RequestVoteRequest(CandidateId: AnotherNodeId, CandidateTerm: term.Increment(),
            LastLogEntryInfo: node.PersistenceFacade.LastEntry);

        node.Handle(request);

        heartbeatTimer.Verify(x => x.Stop(), Times.Once());
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void ПриОбрабаткеЗапросаRequestVote__СТермомНеБольшеСвоего__ДолженОтветитьОтрицательно(
        int myTerm,
        int otherTerm)
    {
        var term = new Term(myTerm);

        using var node = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(CandidateId: AnotherNodeId, CandidateTerm: new(otherTerm),
            LastLogEntryInfo: node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.VoteGranted);
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
    public void ПриОбрабаткеЗапросаHeartbeat__СТермомМеньшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var term = new Term(myTerm);

        using var node = CreateLeaderNode(term, null);

        var request = AppendEntriesRequest.Heartbeat(new(otherTerm), node.PersistenceFacade.CommitIndex, AnotherNodeId,
            node.PersistenceFacade.LastEntry);

        var response = node.Handle(request);

        Assert.False(response.Success);
    }

    [Fact]
    public void ПриОбрабаткеЗапросаHeartbeat__СБолееВысокимТермом__ПослеПереходаВFollowerДолженСброситьHeartbeatТаймер()
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Start());
            t.Setup(x => x.Stop()).Verifiable();
        });

        var log = new Mock<IPersistenceFacade>().Apply(l =>
        {
            throw new NotImplementedException();
        });

        using var node = CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object, log: log.Object);

        var request = AppendEntriesRequest.Heartbeat(term.Increment(), node.PersistenceFacade.CommitIndex,
            new NodeId(2), node.PersistenceFacade.LastEntry);

        node.Handle(request);

        heartbeatTimer.Verify(x => x.Stop(), Times.Once());
    }

    private class SingleHeartbeatRequestQueue : IRequestQueue
    {
        private readonly int _lastLogIndex;
        private readonly TaskCompletionSource _tcs = new();
        private bool _isFirstRequest = true;

        public SingleHeartbeatRequestQueue(int lastLogIndex)
        {
            _lastLogIndex = lastLogIndex;
        }

        public async IAsyncEnumerable<LogReplicationRequest> ReadAllRequestsAsync(
            [EnumeratorCancellation] CancellationToken token)
        {
            await _tcs.Task;
            yield return new LogReplicationRequest(AlwaysTrueQuorumChecker.Instance, _lastLogIndex);
        }

        public void AddHeartbeatIfEmpty()
        {
            if (_isFirstRequest)
            {
                _isFirstRequest = false;
                return;
            }

            _tcs.SetResult();
        }

        public void AddAppendEntries(LogReplicationRequest synchronizer)
        {
        }
    }

    private class SingleHeartbeatRequestQueueFactory : IRequestQueueFactory
    {
        private readonly int _lastLogEntry;

        public SingleHeartbeatRequestQueueFactory(int lastLogEntry)
        {
            _lastLogEntry = lastLogEntry;
        }

        public IRequestQueue CreateQueue()
        {
            return new SingleHeartbeatRequestQueue(_lastLogEntry);
        }
    }

    [Fact]
    public async Task ПриОтправкеHeartbeat__КогдаУзелОтветилОтрицательноИЕгоТермБольше__ДолженПерейтиВFollower()
    {
        throw new NotImplementedException();
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });
        var jobQueue = new SingleRunBackgroundJobQueue();
        var peerTerm = term.Increment();
        var peer = new Mock<IPeer>()
           .Apply(p => p.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                             It.IsAny<CancellationToken>()))
                        .ReturnsAsync(new AppendEntriesResponse(peerTerm.Increment(), false))
                        .Verifiable());

        using var node = CreateLeaderNode(term,
            null,
            new[] {peer.Object},
            heartbeatTimer: heartbeatTimer.Object,
            jobQueue: jobQueue,
            requestQueueFactory: new SingleHeartbeatRequestQueueFactory(0));

        jobQueue.Run();

        heartbeatTimer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }
}