using System.Net.Sockets;
using Moq;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;
using Raft.Core.Peer;
using Raft.Core.StateMachine;

namespace Raft.Core.Tests;

public class LeaderStateTests
{
    private static INode CreateNode(Term currentTerm, PeerId? votedFor, IEnumerable<IPeer>? peers = null)
    {
        return Helpers.CreateNode(currentTerm, votedFor, peers);
    }

    private static RaftStateMachine CreateCandidateStateMachine(INode node, ITimer? electionTimer = null, ITimer? heartbeatTimer = null, IJobQueue? jobQueue = null, ILog? log = null)
    {
        var raftStateMachine = Helpers.CreateStateMachine(node, electionTimer, heartbeatTimer: heartbeatTimer, jobQueue: jobQueue, log: log);
        raftStateMachine.CurrentState = new LeaderState(raftStateMachine, Helpers.NullLogger);
        return raftStateMachine;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void ПриСрабатыванииHeartbeatTimer__ДолженОтправитьHeartbeatНаВсеДругиеУзлы(int peersCount)
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });

        var peers = Enumerable.Range(0, peersCount)
                              .Select(_ => new Mock<IPeer>()
                                  .Apply(p => p.Setup(x => x.SendHeartbeat(It.IsAny<HeartbeatRequest>(),
                                                    It.IsAny<CancellationToken>()))
                                               .ReturnsAsync(new HeartbeatResponse(term, true))
                                               .Verifiable()))
                              .ToList();
        var node = CreateNode(term, null, peers.Select(x => x.Object));
        using var stateMachine = CreateCandidateStateMachine(node, heartbeatTimer: heartbeatTimer.Object);
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        peers.ForEach(p => p.Verify(x => x.SendHeartbeat(It.IsAny<HeartbeatRequest>(), It.IsAny<CancellationToken>()), Times.Once()));
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
                                  .Apply(p => p.Setup(x => x.SendHeartbeat(It.IsAny<HeartbeatRequest>(),
                                                    It.IsAny<CancellationToken>()))
                                               .ReturnsAsync(new HeartbeatResponse(term, true))
                                               .Verifiable()))
                              .ToList();
        var node = CreateNode(term, null, peers.Select(x => x.Object));
        using var stateMachine = CreateCandidateStateMachine(node, heartbeatTimer: heartbeatTimer.Object);
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        heartbeatTimer.Verify(x => x.Start(), Times.Once());
    }

    [Fact]
    public void ПриОбрабаткеЗапросаRequestVote__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var term = new Term(1);

        var node = CreateNode(term, null);
        using var raft = CreateCandidateStateMachine(node);

        var request = new RequestVoteRequest(CandidateId: node.Id + 1, CandidateTerm: term.Increment(),
            LastLog: raft.Log.LastLogEntry);

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

        var node = CreateNode(term, null);
        using var raft = CreateCandidateStateMachine(node, heartbeatTimer: heartbeatTimer.Object);

        var request = new RequestVoteRequest(CandidateId: node.Id + 1, CandidateTerm: term.Increment(),
            LastLog: raft.Log.LastLogEntry);

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

        var node = CreateNode(term, null);
        using var raft = CreateCandidateStateMachine(node);

        var request = new RequestVoteRequest(CandidateId: node.Id + 1, CandidateTerm: new(otherTerm),
            LastLog: raft.Log.LastLogEntry);

        var response = raft.Handle(request);
        
        Assert.False(response.VoteGranted);
    }
    
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(5, 1)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    [InlineData(5, 5)]
    public void ПриОбрабаткеЗапросаHeartbeat__СТермомНеБольшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var term = new Term(myTerm);

        var node = CreateNode(term, null);
        using var raft = CreateCandidateStateMachine(node);

        var request = new HeartbeatRequest(LeaderId: node.Id + 1, Term: new(otherTerm),
            LeaderCommit: raft.Log.CommitIndex, PrevLogEntry: raft.Log.LastLogEntry);

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

        var node = CreateNode(term, null);
        using var raft = CreateCandidateStateMachine(node, heartbeatTimer: heartbeatTimer.Object);

        var request = new HeartbeatRequest(LeaderId: new PeerId(2), Term: term.Increment(),
            PrevLogEntry: raft.Log.LastLogEntry, LeaderCommit: raft.Log.CommitIndex);

        raft.Handle(request);
        
        heartbeatTimer.Verify(x => x.Stop(), Times.Once());
    }
}