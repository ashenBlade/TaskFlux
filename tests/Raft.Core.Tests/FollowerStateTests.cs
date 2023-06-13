using Moq;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Log;
using Raft.Core.StateMachine;

namespace Raft.Core.Tests;

public class FollowerStateTests
{
    private static FollowerState CreateState(IStateMachine stateMachine) => new(stateMachine, Helpers.NullLogger);
    private static readonly PeerId NodeId = new(1);
    private static readonly LogEntry LastLogEntry = new(new Term(1), 0);

    private static ILog CreateLog(LogEntry? logEntryInfo = null, LogEntryCheckResult result = LogEntryCheckResult.Contains, int commitIndex = 0, int lastApplied = 0)
    {
        return Helpers.CreateLog(logEntryInfo, result, commitIndex, lastApplied);
    }
    private static INode CreateNode(Term currentTerm, PeerId? votedFor)
    {
        return Helpers.CreateNode(currentTerm, votedFor);
    }

    private static RaftStateMachine CreateStateMachine(INode node, ITimer? electionTimer = null, IJobQueue? jobQueue = null, ILog? log = null)
    {
        return Helpers.CreateStateMachine(node, electionTimer, jobQueue: jobQueue, log: log);
    }

    [Fact]
    public void ПриСоздании__ПервоеСостояниеДолжноБыть__Follower()
    {
        var machine = RaftStateMachine.Start(CreateNode(new(1), null), Helpers.NullLogger, Helpers.NullTimer, Helpers.NullTimer, Helpers.NullJobQueue, Mock.Of<ILog>(x => x.LastLogEntry == LastLogEntry && x.CommitIndex == 0 && x.LastApplied == 0));
        Assert.Equal(NodeRole.Follower, machine.CurrentRole);
    }
    
    [Fact]
    public void ПриЗапросеRequestVoteСБолееВысокимТермом__КогдаПреждеНеГолосовал__ДолженВыставитьСвойТермВБолееВысокий()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var stateMachine = CreateStateMachine(node);
        var state = CreateState(stateMachine);
        
        var expectedTerm = oldTerm.Increment();
        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = expectedTerm, 
            LastLog = new LogEntry(oldTerm, 0)
        };

        state.Apply(request);
        
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }
    
    [Fact]
    public void ПриЗапросеRequestVote__СБолееВысокимТермом__ДолженОтветитьПоложительно()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var stateMachine = CreateStateMachine(node);
        
        var state = CreateState(stateMachine);
        var expectedTerm = oldTerm.Increment();
        
        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = expectedTerm, 
            LastLog = new LogEntry(oldTerm, 0)
        };

        var response = state.Apply(request);
        
        Assert.True(response.VoteGranted);
    }
    
    
    [Theory]
    [InlineData(2, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(3, 1)]
    [InlineData(3, 3)]
    public void ПриЗапросеRequestVote__СТермомНеБольшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var oldTerm = new Term(myTerm);
        
        var node = CreateNode(oldTerm, null);
        var stateMachine = CreateStateMachine(node);
        var state = CreateState(stateMachine);
        
        var expectedTerm = new Term(otherTerm);
        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = expectedTerm, 
            LastLog = new LogEntry(oldTerm, 0)
        };

        var response = state.Apply(request);
        
        Assert.False(response.VoteGranted);
    }
    
    [Fact]
    public void ПриЗапросеRequestVote__ДолженПерезапуститьElectionTimeout()
    {
        var node = CreateNode(new Term(1), null);
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Reset())
             .Verifiable();
        var stateMachine = CreateStateMachine(node, electionTimer: timer.Object);
        var state = CreateState(stateMachine);

        state.Apply(new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = new Term(1),
            LastLog = new LogEntry(new(1), 0)
        });
        
        timer.Verify(x => x.Reset(), Times.Once());
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ДолженПерейтиВСостояниеCandidate()
    {
        var node = CreateNode(new Term(1), null);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        var stateMachine = CreateStateMachine(node, timer.Object);
        var state = CreateState(stateMachine);

        timer.Raise(x => x.Timeout += null);
        
        Assert.Equal(NodeRole.Candidate, stateMachine.CurrentRole);
        GC.KeepAlive(state);
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ПослеСрабатыванияОбработчика__ДолженПерейтиВСледующийТерм()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var stateMachine = CreateStateMachine(node, timer.Object);

        timer.Raise(x => x.Timeout += null);

        var expectedTerm = oldTerm.Increment();
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ПослеСрабатыванияОбработчика__ДолженПроголосоватьЗаСебя()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var stateMachine = CreateStateMachine(node, timer.Object);

        timer.Raise(x => x.Timeout += null);

        Assert.Equal(node.Id, node.VotedFor);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void ПриЗапросеHeartbeat__ДолженСбрасыватьElectionTimeout(int term)
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        timer.Setup(x => x.Reset()).Verifiable();
        using var stateMachine = CreateStateMachine(node, electionTimer: timer.Object, log: CreateLog());

        var request = new HeartbeatRequest()
        {
            Term = new Term(term),
            LeaderCommit = 0,
            LeaderId = new PeerId(Value: NodeId. Value+ 1),
            PrevLogEntry = new LogEntry(new Term(term), 0) 
        };

        stateMachine.Handle(request);

        var exception = Record.Exception(() => timer.Verify(x => x.Reset(), Times.Once()));
        Assert.Null(exception);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void ПриЗапросеHeartbeat__СБольшимТермомИВалиднымЖурналом__ДолженОбновитьСвойТерм(int term)
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        timer.Setup(x => x.Reset()).Verifiable();
        using var raft = CreateStateMachine(node, electionTimer: timer.Object);

        var leaderTerm = new Term(term);
        var request = new HeartbeatRequest()
        {
            Term = leaderTerm,
            LeaderCommit = 0,
            LeaderId = new PeerId(Value: NodeId. Value + 1),
            PrevLogEntry = new LogEntry(new Term(term), 0) 
        };

        raft.Handle(request);
        
        Assert.Equal(leaderTerm, node.CurrentTerm);
    }
    
}