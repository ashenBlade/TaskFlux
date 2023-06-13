using Moq;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Log;
using Raft.Core.Peer;
using Raft.Core.StateMachine;

namespace Raft.Core.Tests;

public class CandidateStateTests
{
    private class SingleRunJobQueue : IJobQueue
    {
        public (Func<Task> Job, CancellationToken Token)? Job { get; set; }
        public void EnqueueInfinite(Func<Task> job, CancellationToken token)
        {
            Job = (job, token);
        }

        public async Task Run()
        {
            if (Job is not {Job: {} job})
            {
                throw new ArgumentNullException(nameof(Job), "Задача не была зарегистрирована");
            }
            

            await job();
        }
    }

    private static INode CreateNode(Term currentTerm, PeerId? votedFor, IEnumerable<IPeer>? peers = null)
    {
        return Helpers.CreateNode(currentTerm, votedFor, peers);
    }

    private static RaftStateMachine CreateCandidateStateMachine(INode node, ITimer? electionTimer = null, IJobQueue? jobQueue = null, ILog? log = null)
    {
        var raftStateMachine = Helpers.CreateStateMachine(node, electionTimer, heartbeatTimer: null, jobQueue, log);
        raftStateMachine.CurrentState = new CandidateState(raftStateMachine, Helpers.NullLogger);
        return raftStateMachine;
    }

    [Fact]
    public void ПриСрабатыванииElectionTimer__ДолженПерейтиВСледующийТерм()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var electionTimer = new Mock<ITimer>();
        using var stateMachine = CreateCandidateStateMachine(node, electionTimer: electionTimer.Object);
        
        electionTimer.Raise(x => x.Timeout += null);

        var nextTerm = oldTerm.Increment();
        Assert.Equal(nextTerm, node.CurrentTerm);
    }

    [Fact]
    public void ПриСрабатыванииElectionTimer__ДолженОстатьсяВCandidateСостоянии()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var electionTimer = new Mock<ITimer>();
        using var raft = CreateCandidateStateMachine(node, electionTimer: electionTimer.Object);
        
        electionTimer.Raise(x => x.Timeout += null);

        Assert.Equal(NodeRole.Candidate, raft.CurrentRole);
    }

    [Fact]
    public async Task КогдаВКластереНетДругихУзлов__ПослеЗапускаКворума__ДолженПерейтиВСостояниеЛидера()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var jobQueue = new SingleRunJobQueue();
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue);

        await jobQueue.Run();

        Assert.Equal(NodeRole.Leader, raft.CurrentRole);
    }
    
    [Fact]
    public async Task ПослеЗапускаКворумаИПереходаВСостояниеЛидера__ДолженОстановитьElectionTimeout()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var jobQueue = new SingleRunJobQueue();
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Stop()).Verifiable();
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue, electionTimer: timer.Object);

        await jobQueue.Run();

        timer.Verify(x => x.Stop(), Times.Once());
    }

    // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ПриЗапускеКворума__СЕдинственнымДругимУзлом__ДолженСтатьЛидеромТолькоКогдаДругойУзелОтдалСвойГолос(bool voteGranted)
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunJobQueue();
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Stop()).Verifiable();
        
        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RequestVoteResponse() {CurrentTerm = oldTerm, VoteGranted = voteGranted});
        
        var node = CreateNode(oldTerm, null, peers: new[] {peer.Object});
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue, electionTimer: timer.Object);
        
        await jobQueue.Run();

        if (voteGranted)
        {
            Assert.Equal(NodeRole.Leader, raft.CurrentRole);
        }
        else
        {
            Assert.Equal(NodeRole.Candidate, raft.CurrentRole);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ПриЗапускеКворума__СЕдинственнымДругимУзлом__ДолженПрекратитьОтправлятьЗапросыПослеПолученияОтвета(bool voteGranted)
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunJobQueue();
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Stop()).Verifiable();
        
        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RequestVoteResponse() {CurrentTerm = oldTerm, VoteGranted = voteGranted});
        
        var node = CreateNode(oldTerm, null, peers: new[] {peer.Object});
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue, electionTimer: timer.Object);
        
        await jobQueue.Run();
        
        peer.Verify(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()), Times.Once());
    }
    
    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    [InlineData(3, 1)]
    [InlineData(5, 5)]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(3, 3)]
    public async Task ПриЗапускеКворума__СНесколькимиУзлами__ДолженСтатьЛидеромКогдаСобралКворум(int grantedVotesCount, int nonGrantedVotesCount)
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunJobQueue();
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Stop()).Verifiable();


        var peers = Random.Shared.Shuffle(
            Enumerable.Range(0, grantedVotesCount + nonGrantedVotesCount)
                      .Select(i =>
                       {
                           var mock = new Mock<IPeer>();
                           mock.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(),
                                    It.IsAny<CancellationToken>()))
                               .ReturnsAsync(new RequestVoteResponse()
                                {
                                    CurrentTerm = oldTerm, 
                                    VoteGranted = i < grantedVotesCount
                                });
                           return mock.Object;
                       })
                      .ToArray() );
        
        var node = CreateNode(oldTerm, null, peers: peers);
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue, electionTimer: timer.Object);
        
        await jobQueue.Run();
        
        Assert.Equal(NodeRole.Leader, raft.CurrentRole);
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
    public async Task ПриЗапускеКворума__СНесколькимиУзлами__НеДолженСтатьЛидеромКогдаНеСобралКворум(
        int grantedVotesCount,
        int nonGrantedVotesCount)
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunJobQueue();
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Stop()).Verifiable();


        var peers = Random.Shared.Shuffle(
            Enumerable.Range(0, grantedVotesCount + nonGrantedVotesCount)
                      .Select(i =>
                       {
                           var mock = new Mock<IPeer>();
                           mock.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
                               .ReturnsAsync(new RequestVoteResponse()
                                {
                                    CurrentTerm = oldTerm, 
                                    VoteGranted = i < grantedVotesCount
                                });
                           return mock.Object;
                       })
                      .ToArray() );
        
        var node = CreateNode(oldTerm, null, peers: peers);
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue, electionTimer: timer.Object);
        
        await jobQueue.Run();
        
        Assert.Equal(NodeRole.Candidate, raft.CurrentRole);
    }

    [Fact]
    public async Task ПриЗапускеКворума__СЕдинственнымУзломКоторыйНеОтветил__ДолженСделатьПовторнуюПопытку()
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunJobQueue();
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Stop()).Verifiable();

        var mock = new Mock<IPeer>();
        var calledFirstTime = true;
        mock.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((RequestVoteRequest _, CancellationToken _) =>
             {
                 if (calledFirstTime)
                 {
                     calledFirstTime = false;
                     return null;
                 }
                 return new RequestVoteResponse()
                 {
                     CurrentTerm = oldTerm,
                     VoteGranted = true
                 };
             });
        
        var node = CreateNode(oldTerm, null, peers: new []{mock.Object});
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue, electionTimer: timer.Object);
        
        await jobQueue.Run();
        
        mock.Verify(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }
    
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ПриЗапускеКворума__СЕдинственнымУзломКоторыйОтветилТолькоНаВторойПопытке__ДолженСтатьЛидеромТолькоЕслиОтдалГолос(bool voteGranted)
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunJobQueue();
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Stop()).Verifiable();

        var mock = new Mock<IPeer>();
        var calledFirstTime = true;
        mock.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((RequestVoteRequest _, CancellationToken _) =>
             {
                 if (calledFirstTime)
                 {
                     calledFirstTime = false;
                     return null;
                 }
                 return new RequestVoteResponse()
                 {
                     CurrentTerm = oldTerm,
                     VoteGranted = voteGranted
                 };
             });
        
        var node = CreateNode(oldTerm, null, peers: new []{mock.Object});
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue, electionTimer: timer.Object);
        
        await jobQueue.Run();

        Assert.Equal(voteGranted ? NodeRole.Leader : NodeRole.Candidate, raft.CurrentRole);
    }
    
    [Fact]
    public void ПриОбработкеRequestVote__СБолееВысокимТермом__ДолженСтатьFollower()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        using var raft = CreateCandidateStateMachine(node);

        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = oldTerm.Increment(),
            LastLog = raft.Log.LastLogEntry
        };

        raft.Handle(request);

        Assert.Equal(NodeRole.Follower, raft.CurrentRole);
    }

    [Fact]
    public void ПриОбработкеRequestVote__СБолееВысокимТермом__ДолженОбновитьСвойТерм()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        using var raft = CreateCandidateStateMachine(node);

        var requestTerm = oldTerm.Increment();
        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = requestTerm,
            LastLog = raft.Log.LastLogEntry
        };

        raft.Handle(request);

        Assert.Equal(requestTerm, raft.Node.CurrentTerm);
    }
    
    [Fact]
    public void ПриОбработкеHeartbeat__СБолееВысокимТермом__ДолженПерейтиВFollower()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        using var raft = CreateCandidateStateMachine(node);

        var request = new HeartbeatRequest()
        {
            Term = oldTerm.Increment(),
            LeaderCommit = raft.Log.CommitIndex,
            LeaderId = new PeerId(2),
            PrevLogEntry = raft.Log.LastLogEntry
        };

        raft.Handle(request);

        Assert.Equal(NodeRole.Follower, raft.CurrentRole);
    }
    
    [Fact]
    public void ПриОбработкеHeartbeat__СБолееВысокимТермом__ДолженОбновитьСвойТерм()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        using var raft = CreateCandidateStateMachine(node);

        var leaderTerm = oldTerm.Increment();
        var request = new HeartbeatRequest()
        {
            Term = leaderTerm,
            LeaderCommit = raft.Log.CommitIndex,
            LeaderId = new PeerId(2),
            PrevLogEntry = raft.Log.LastLogEntry
        };

        raft.Handle(request);

        Assert.Equal(leaderTerm, node.CurrentTerm);
    }
    
    [Fact]
    public async Task ПриОбработкеHeartbeat__СБолееВысокимТермомИСобраннымКворумом__ДолженСтатьFollower()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var jobQueue = new SingleRunJobQueue();
        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RequestVoteResponse() {CurrentTerm = oldTerm, VoteGranted = true});
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue);

        var leaderTerm = oldTerm.Increment();
        var request = new HeartbeatRequest()
        {
            Term = leaderTerm,
            LeaderCommit = raft.Log.CommitIndex,
            LeaderId = new PeerId(2),
            PrevLogEntry = raft.Log.LastLogEntry
        };

        raft.Handle(request);
        await jobQueue.Run();

        Assert.Equal(NodeRole.Follower, raft.CurrentRole);
    }
    
    [Fact]
    public async Task ПриОбработкеRequestVote__СБолееВысокимТермомИСобраннымКворумом__ДолженСтатьFollower()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var jobQueue = new SingleRunJobQueue();
        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RequestVoteResponse() {CurrentTerm = oldTerm, VoteGranted = true});
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue);

        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2),
            CandidateTerm = oldTerm.Increment(),
            LastLog = raft.Log.LastLogEntry
        };

        raft.Handle(request);
        await jobQueue.Run();

        Assert.Equal(NodeRole.Follower, raft.CurrentRole);
    }
    
    
    [Fact]
    public async Task ПриОбработкеRequestVote__СБолееВысокимТермомИСобраннымКворумом__ДолженОбновитьСвойТерм()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var jobQueue = new SingleRunJobQueue();
        var peer = new Mock<IPeer>();
        var leaderTerm = oldTerm.Increment();
        peer.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RequestVoteResponse() {CurrentTerm = oldTerm, VoteGranted = true});
        using var raft = CreateCandidateStateMachine(node, jobQueue: jobQueue);

        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2),
            CandidateTerm = leaderTerm,
            LastLog = raft.Log.LastLogEntry
        };

        raft.Handle(request);
        await jobQueue.Run();

        Assert.Equal(NodeRole.Follower, raft.CurrentRole);
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    public async Task ПослеПереходаВLeader__КогдаКворумСобран__ДолженОстановитьElectionТаймер(int votes)
    {
        var oldTerm = new Term(1);
        var jobQueue = new SingleRunJobQueue();
        
        var peer = new Mock<IPeer>();
        peer.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RequestVoteResponse() {CurrentTerm = oldTerm, VoteGranted = true});

        var electionTimer = new Mock<ITimer>().Apply(t =>
        {
            t.Setup(x => x.Stop()).Verifiable();
        });
        
        
        var peers = Enumerable.Range(0, votes)
                              .Select(_ => new Mock<IPeer>()
                                          .Apply(t => t.Setup(x => x.SendRequestVote(It.IsAny<RequestVoteRequest>(),
                                                            It.IsAny<CancellationToken>()))
                                                       .ReturnsAsync(new RequestVoteResponse()
                                                        {
                                                            CurrentTerm = oldTerm, VoteGranted = true
                                                        }))
                                          .Object)
                              .ToArray();
        var node = CreateNode(oldTerm, null, peers);
        using var raft = CreateCandidateStateMachine(node, electionTimer: electionTimer.Object, jobQueue: jobQueue);

        await jobQueue.Run();
        
        electionTimer.Verify(x => x.Stop(), Times.Once());
    }
}