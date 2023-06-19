using System.Net.Sockets;
using Moq;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;
using Raft.Core.Node;

namespace Raft.Core.Tests;

public class LeaderStateTests
{
    private static RaftNode CreateCandidateStateMachine(Term currentTerm, NodeId? votedFor, IEnumerable<IPeer>? peers = null, ITimer? electionTimer = null, ITimer? heartbeatTimer = null, IJobQueue? jobQueue = null, ILog? log = null)
    {
        var raftStateMachine = Helpers.CreateStateMachine(currentTerm, votedFor, peers: peers, electionTimer: electionTimer, heartbeatTimer: heartbeatTimer, jobQueue: jobQueue, log: log);
        ( ( INode ) raftStateMachine ).CurrentState = new LeaderState(raftStateMachine, Helpers.NullLogger);
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
        using var stateMachine = CreateCandidateStateMachine(term, null, peers.Select(x => x.Object), heartbeatTimer: heartbeatTimer.Object);
        
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
        using var stateMachine = CreateCandidateStateMachine(term, null, peers.Select(x => x.Object), heartbeatTimer: heartbeatTimer.Object);
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        heartbeatTimer.Verify(x => x.Start(), Times.Once());
    }

    [Fact]
    public void ПриОбрабаткеЗапросаRequestVote__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var term = new Term(1);

        using var raft = CreateCandidateStateMachine(term, null);

        var request = new RequestVoteRequest(CandidateId: raft.Id + 1, CandidateTerm: term.Increment(),
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

        using var raft = CreateCandidateStateMachine(term, null, heartbeatTimer: heartbeatTimer.Object);

        var request = new RequestVoteRequest(CandidateId: raft.Id + 1, CandidateTerm: term.Increment(),
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

        using var raft = CreateCandidateStateMachine(term, null);

        var request = new RequestVoteRequest(CandidateId: raft.Id + 1, CandidateTerm: new(otherTerm),
            LastLog: raft.Log.LastLogEntry);

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

        using var raft = CreateCandidateStateMachine(term, null);

        var request = new HeartbeatRequest(LeaderId: raft.Id + 1, Term: new(otherTerm),
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

        using var raft = CreateCandidateStateMachine(term, null, heartbeatTimer: heartbeatTimer.Object);

        var request = new HeartbeatRequest(LeaderId: new NodeId(2), Term: term.Increment(),
            PrevLogEntry: raft.Log.LastLogEntry, LeaderCommit: raft.Log.CommitIndex);

        raft.Handle(request);
        
        heartbeatTimer.Verify(x => x.Stop(), Times.Once());
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
        var peerTerm = term.Increment();
        var peer = new Mock<IPeer>()
           .Apply(p => p.Setup(x => x.SendHeartbeat(It.IsAny<HeartbeatRequest>(),
                             It.IsAny<CancellationToken>()))
                        .ReturnsAsync(new HeartbeatResponse(peerTerm, false))
                        .Verifiable());
        
        using var stateMachine = CreateCandidateStateMachine(term, null, new[] {peer.Object}, heartbeatTimer: heartbeatTimer.Object);
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Follower, stateMachine.CurrentRole);
    }

    [Theory]
    [InlineData(2, 3, 4, 5)]
    [InlineData(5, 4, 3, 2)]
    [InlineData(2, 4343, 23, 23)]
    public async Task
        ПриОтправкеHeartbeat__КогдаНесколькоУзловОтветилоОтрицательноСНесколькимиРазнымиТермамиБольшеМоего__ДолженПерейтиВНаибольшийИзВернувшихсяТермов(params int[] terms)
    {
        var term = new Term(1);
        var heartbeatTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
            t.Setup(x => x.Start());
        });
        var peers = terms.Select(t =>
                          {
                              var peer = new Mock<IPeer>();
                              peer.Setup(p => p.SendHeartbeat(It.IsAny<HeartbeatRequest>(),
                                       It.IsAny<CancellationToken>()))
                                  .ReturnsAsync(new HeartbeatResponse(new Term(t), false));
                              return peer.Object;
                          })
                         .ToArray();
        var maxTerm = new Term(terms.Max());
        using var stateMachine = CreateCandidateStateMachine(term, null, peers, heartbeatTimer: heartbeatTimer.Object);
        
        heartbeatTimer.Raise(x => x.Timeout += null);

        Assert.Equal(maxTerm, stateMachine.CurrentTerm);
    }
}