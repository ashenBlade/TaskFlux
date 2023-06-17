using Moq;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;
using Raft.Core.Node;

namespace Raft.Core.Tests;

public class FollowerStateTests
{
    // private static FollowerState CreateState(INode node) => new(node, Helpers.NullLogger);
    private static readonly NodeId NodeId = new(1);
    private static readonly LogEntry LastLogEntry = new(new Term(1), 0);

    private static ILog CreateLog(LogEntry? logEntryInfo = null, LogEntryCheckResult result = LogEntryCheckResult.Contains, int commitIndex = 0, int lastApplied = 0)
    {
        return Helpers.CreateLog(logEntryInfo, result, commitIndex, lastApplied);
    }

    private static RaftNode CreateNode(Term currentTerm, NodeId? votedFor, ITimer? electionTimer = null, IJobQueue? jobQueue = null, ILog? log = null)
    {
        return Helpers.CreateStateMachine(currentTerm, votedFor, electionTimer: electionTimer, jobQueue: jobQueue, log: log);
    }

    [Fact]
    public void ПриСоздании__ПервоеСостояниеДолжноБыть__Follower()
    {
        var machine = RaftNode.Create(new(1), new PeerGroup(Array.Empty<IPeer>()), null, new(1), Helpers.NullLogger, Helpers.NullTimer, Helpers.NullTimer, Helpers.NullJobQueue, Mock.Of<ILog>(x => x.LastLogEntry == LastLogEntry && x.CommitIndex == 0 && x.LastApplied == 0), Helpers.DefaultCommandQueue);
        Assert.Equal(NodeRole.Follower, machine.CurrentRole);
    }
    
    [Fact]
    public void ПриЗапросеRequestVoteСБолееВысокимТермом__КогдаПреждеНеГолосовал__ДолженВыставитьСвойТермВБолееВысокий()
    {
        var oldTerm = new Term(1);
        using var raft = CreateNode(oldTerm, null);
        
        var expectedTerm = oldTerm.Increment();
        var request = new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: expectedTerm,
            LastLog: new LogEntry(oldTerm, 0));

        raft.Handle(request);
        
        Assert.Equal(expectedTerm, raft.CurrentTerm);
    }
    
    [Fact]
    public void ПриЗапросеRequestVote__СБолееВысокимТермом__ДолженОтветитьПоложительно()
    {
        var oldTerm = new Term(1);
        var stateMachine = CreateNode(oldTerm, null);
        
        var expectedTerm = oldTerm.Increment();
        
        var request = new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: expectedTerm,
            LastLog: new LogEntry(oldTerm, 0));

        var response = stateMachine.Handle(request);
        
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
        
        var stateMachine = CreateNode(oldTerm, null);
        
        var expectedTerm = new Term(otherTerm);
        var request = new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: expectedTerm,
            LastLog: new LogEntry(oldTerm, 0));

        var response = stateMachine.Handle(request);
        
        Assert.False(response.VoteGranted);
    }
    
    [Fact]
    public void ПриЗапросеRequestVote__ДолженПерезапуститьElectionTimeout()
    {
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Reset())
             .Verifiable();
        var stateMachine = CreateNode(new Term(1), null, electionTimer: timer.Object);

        stateMachine.Handle(new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: new Term(1),
            LastLog: new LogEntry(new(1), 0)));
        
        timer.Verify(x => x.Reset(), Times.Once());
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ДолженПерейтиВСостояниеCandidate()
    {
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        var stateMachine = CreateNode(new Term(1), null, electionTimer: timer.Object);

        timer.Raise(x => x.Timeout += null);
        
        Assert.Equal(NodeRole.Candidate, stateMachine.CurrentRole);
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ПослеСрабатыванияОбработчика__ДолженПерейтиВСледующийТерм()
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var raft = CreateNode(new Term(1), null, timer.Object);

        timer.Raise(x => x.Timeout += null);

        var expectedTerm = oldTerm.Increment();
        Assert.Equal(expectedTerm, raft.CurrentTerm);
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ПослеСрабатыванияОбработчика__ДолженПроголосоватьЗаСебя()
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var stateMachine = CreateNode(oldTerm, null, timer.Object);

        timer.Raise(x => x.Timeout += null);

        Assert.Equal(stateMachine.Id, stateMachine.VotedFor);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void ПриЗапросеHeartbeat__ДолженСбрасыватьElectionTimeout(int term)
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        timer.Setup(x => x.Reset()).Verifiable();
        using var stateMachine = CreateNode(oldTerm, null, electionTimer: timer.Object, log: CreateLog());

        var request = new HeartbeatRequest(Term: new Term(term), LeaderCommit: 0,
            LeaderId: new NodeId(Value: NodeId.Value + 1), PrevLogEntry: new LogEntry(new Term(term), 0));

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
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        timer.Setup(x => x.Reset()).Verifiable();
        using var raft = CreateNode(oldTerm, null, electionTimer: timer.Object);

        var leaderTerm = new Term(term);
        var request = new HeartbeatRequest(Term: leaderTerm, LeaderCommit: 0,
            LeaderId: new NodeId(Value: NodeId.Value + 1), PrevLogEntry: new LogEntry(new Term(term), 0));

        raft.Handle(request);
        
        Assert.Equal(leaderTerm, raft.CurrentTerm);
    }

    [Fact]
    public void ПриЗапросеRequestVote__СБолееВысокимТермом__ДолженОтдатьГолосЗаКандидата()
    {
        var oldTerm = new Term(1);
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        timer.Setup(x => x.Reset()).Verifiable();
        using var raft = CreateNode(oldTerm, null, electionTimer: timer.Object);

        var candidateId = new NodeId(2);
        var request = new RequestVoteRequest(CandidateId: candidateId, CandidateTerm: oldTerm.Increment(),
            LastLog: raft.Log.LastLogEntry);
        raft.Handle(request);
        Assert.Equal(candidateId, raft.VotedFor);
    }
    
    [Theory]
    [InlineData(null)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void ПриЗапросеHeartbeat__СБолееВысокимТермом__ДолженВыставитьСвойГолосВnull(int? oldVotedFor)
    {
        var oldTerm = new Term(1);
        NodeId? votedForId = oldVotedFor is null
                                 ? null
                                 : new NodeId(oldVotedFor.Value);
        using var raft = CreateNode(oldTerm, votedForId);

        var request = new HeartbeatRequest(Term: raft.CurrentTerm.Increment(), LeaderCommit: raft.Log.CommitIndex,
            LeaderId: new NodeId(2), PrevLogEntry: raft.Log.LastLogEntry);
        raft.Handle(request);
        
        Assert.False(raft.VotedFor.HasValue);
    }
    
}