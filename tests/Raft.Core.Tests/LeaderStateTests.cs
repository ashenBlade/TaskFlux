using System.Runtime.CompilerServices;
using Moq;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;
using Raft.Core.Node;
using Raft.Core.Node.LeaderState;

namespace Raft.Core.Tests;

public class LeaderStateTests
{
    private static RaftNode CreateLeaderNode(Term currentTerm, NodeId? votedFor, IEnumerable<IPeer>? peers = null, ITimer? electionTimer = null, ITimer? heartbeatTimer = null, IJobQueue? jobQueue = null, ILog? log = null, IRequestQueueFactory? requestQueueFactory = null)
    {
        var raftStateMachine = Helpers.CreateStateMachine(
            currentTerm,
            votedFor,
            peers: peers,
            electionTimer: electionTimer,
            heartbeatTimer: heartbeatTimer,
            jobQueue: jobQueue,
            log: log);
        ( ( INode ) raftStateMachine ).CurrentState = new LeaderState(
            raftStateMachine,
            Helpers.NullLogger, 
            requestQueueFactory ?? new SingleHeartbeatRequestQueueFactory(0));
        return raftStateMachine;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task ПриСрабатыванииHeartbeatTimer__ДолженОтправитьHeartbeatНаВсеДругиеУзлы(int peersCount)
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });

        var jobQueue = new SingleRunJobQueue();

        var peers = Enumerable.Range(0, peersCount)
                              .Select(_ => new Mock<IPeer>()
                                  .Apply(p => p.Setup(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(),
                                                    It.IsAny<CancellationToken>()))
                                               .ReturnsAsync(new AppendEntriesResponse(term, true))
                                               .Verifiable()))
                              .ToList();
        using var stateMachine = CreateLeaderNode(term, 
            null,
            peers.Select(x => x.Object),
            heartbeatTimer: heartbeatTimer.Object,
            jobQueue: jobQueue,
            requestQueueFactory: new SingleHeartbeatRequestQueueFactory(0));
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        await jobQueue.Run();
        
        peers.ForEach(p => p.Verify(x => x.SendAppendEntries(It.IsAny<AppendEntriesRequest>(), It.IsAny<CancellationToken>()), Times.Once()));
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
        using var stateMachine = CreateLeaderNode(term, null, peers.Select(x => x.Object), heartbeatTimer: heartbeatTimer.Object);
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        heartbeatTimer.Verify(x => x.Start(), Times.Once());
    }

    [Fact]
    public void ПриОбрабаткеЗапросаRequestVote__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var term = new Term(1);

        using var raft = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(CandidateId: raft.Id + 1, CandidateTerm: term.Increment(),
            LastLog: raft.Log.LastLogEntryInfo);

        raft.Handle(request);
        
        Assert.Equal(NodeRole.Follower, raft.CurrentRole);
    }
    
    [Fact]
    public void ПриОбрабаткеЗапросаRequestVote__СБолееВысокимТермом__ПослеПереходаВFollowerДолженСброситьHeartbeatТаймер()
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Start());
            t.Setup(x => x.Stop()).Verifiable();
        });

        using var raft = CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object);

        var request = new RequestVoteRequest(CandidateId: raft.Id + 1, CandidateTerm: term.Increment(),
            LastLog: raft.Log.LastLogEntryInfo);

        raft.Handle(request);
        
        heartbeatTimer.Verify(x => x.Stop(), Times.Once());
    }
    
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    [InlineData(5, 5)]
    public void ПриОбрабаткеЗапросаRequestVote__СТермомНеБольшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var term = new Term(myTerm);

        using var raft = CreateLeaderNode(term, null);

        var request = new RequestVoteRequest(CandidateId: raft.Id + 1, CandidateTerm: new(otherTerm),
            LastLog: raft.Log.LastLogEntryInfo);

        var response = raft.Handle(request);
        
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

        using var raft = CreateLeaderNode(term, null);

        var request = AppendEntriesRequest.Heartbeat( new(otherTerm), raft.Log.CommitIndex, raft.Id + 1, raft.Log.LastLogEntryInfo);

        var response = raft.Handle(request);
        
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

        using var raft = CreateLeaderNode(term, null, heartbeatTimer: heartbeatTimer.Object);

        var request = AppendEntriesRequest.Heartbeat(term.Increment(), raft.Log.CommitIndex, new NodeId(2), raft.Log.LastLogEntryInfo);

        raft.Handle(request);
        
        heartbeatTimer.Verify(x => x.Stop(), Times.Once());
    }

    private class SingleHeartbeatRequestQueue : IRequestQueue
    {
        private readonly int _lastLogIndex;
        private readonly TaskCompletionSource _tcs = new();

        public SingleHeartbeatRequestQueue(int lastLogIndex)
        {
            _lastLogIndex = lastLogIndex;
        }
        public async IAsyncEnumerable<AppendEntriesRequestSynchronizer> ReadAllRequestsAsync([EnumeratorCancellation] CancellationToken token)
        {
            await _tcs.Task;
            yield return new AppendEntriesRequestSynchronizer(
                AlwaysTrueQuorumChecker.Instance, _lastLogIndex);
        }

        public void AddHeartbeat()
        {
            _tcs.SetResult();
        }
    }
    
    private class SingleHeartbeatRequestQueueFactory: IRequestQueueFactory
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
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });
        var jobQueue = new SingleRunJobQueue();
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
        
        var task = jobQueue.Run();
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        await task;
        
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }
}