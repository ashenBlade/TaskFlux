using Moq;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Log;

namespace Consensus.Core.Tests;

public class FollowerStateTests
{
    private static readonly NodeId NodeId = new(1);
    private static readonly LogEntryInfo LastLogEntryInfo = new(new Term(1), 0);

    private static ILog CreateLog(LogEntryInfo? logEntryInfo = null, int commitIndex = 0, int lastApplied = 0)
    {
        return Helpers.CreateLog(logEntryInfo, commitIndex, lastApplied);
    }

    private static RaftConsensusModule<int, int> CreateNode(Term currentTerm, NodeId? votedFor, ITimer? electionTimer = null, IJobQueue? jobQueue = null, ILog? log = null)
    {
        return Helpers.CreateNode(currentTerm, 
            votedFor,
            electionTimer: electionTimer,
            jobQueue: jobQueue,
            log: log);
    }

    [Fact]
    public void ПриСоздании__ПервоеСостояниеДолжноБыть__Follower()
    {
        var machine = RaftConsensusModule.Create(new(1), new PeerGroup(Array.Empty<IPeer>()), Helpers.NullLogger, Helpers.NullTimer, Helpers.NullTimer, Helpers.NullJobQueue, Mock.Of<ILog>(x => x.LastEntry == LastLogEntryInfo && x.CommitIndex == 0 && x.LastApplied == 0), Helpers.DefaultCommandQueue, Helpers.NullStateMachine, Helpers.NullMetadataStorage, StubSerializer<int>.Default);
        Assert.Equal(NodeRole.Follower, machine.CurrentRole);
    }
    
    [Fact]
    public void ПриЗапросеRequestVoteСБолееВысокимТермом__КогдаПреждеНеГолосовал__ДолженВыставитьСвойТермВБолееВысокий()
    {
        var oldTerm = new Term(1);
        using var raft = CreateNode(oldTerm, null);
        
        var expectedTerm = oldTerm.Increment();
        var request = new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(oldTerm, 0));

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
            LastLogEntryInfo: new LogEntryInfo(oldTerm, 0));

        var response = stateMachine.Handle(request);
        
        Assert.True(response.VoteGranted);
    }
    
    
    [Theory]
    [InlineData(2, 1)]
    // [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(3, 1)]
    // [InlineData(3, 3)]
    public void ПриЗапросеRequestVote__КогдаНеОтдавалГолосСТермомНеБольшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var oldTerm = new Term(myTerm);
        
        var stateMachine = CreateNode(oldTerm, null);
        
        var expectedTerm = new Term(otherTerm);
        var request = new RequestVoteRequest(CandidateId: new NodeId(2), CandidateTerm: expectedTerm,
            LastLogEntryInfo: new LogEntryInfo(oldTerm, 0));

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
            LastLogEntryInfo: new LogEntryInfo(new(1), 0)));
        
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
        using var node = CreateNode(oldTerm, null, electionTimer: timer.Object, log: CreateLog(logEntryInfo: null));

        var request = AppendEntriesRequest.Heartbeat(new Term(term), 0,
            new NodeId(Value: NodeId.Value + 1), new LogEntryInfo(new Term(term), 0));

        node.Handle(request);

        timer.Verify(x => x.Reset(), Times.Once());
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
        var log = new Mock<ILog>().Apply(l => l.Setup(x => x.Contains(It.IsAny<LogEntryInfo>())).Returns(true));
        using var raft = CreateNode(oldTerm, null, electionTimer: timer.Object, log: log.Object);
        var leaderTerm = new Term(term);
        var request = AppendEntriesRequest.Heartbeat(leaderTerm, 0,
            new NodeId(Value: NodeId.Value + 1), new LogEntryInfo(new Term(term), 0));

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
            LastLogEntryInfo: raft.Log.LastEntry);
        raft.Handle(request);
        Assert.Equal(candidateId, raft.VotedFor);
    }

    private static ILog CreateLog(bool isConsistentWith = true) => 
        new Mock<ILog>()
           .Apply(l =>
            {
                l.Setup(x => x.Contains(It.IsAny<LogEntryInfo>()))
                 .Returns(isConsistentWith);
            })
           .Object;

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
        
        using var node = CreateNode(oldTerm, votedForId, log: CreateLog(true));

        var request = AppendEntriesRequest.Heartbeat(node.CurrentTerm.Increment(), node.Log.CommitIndex, new NodeId(2), node.Log.LastEntry);
        node.Handle(request);
        
        Assert.False(node.VotedFor.HasValue);
    }

    [Fact]
    public void ПриЗапросеHeartbeat__СБолееВысокимТермом__ДолженОстатьсяFollower()
    {
        var oldTerm = new Term(1);
        
        using var node = CreateNode(oldTerm, null, log: CreateLog(isConsistentWith: true));

        var request = AppendEntriesRequest.Heartbeat(node.CurrentTerm.Increment(), node.Log.CommitIndex, new NodeId(2), node.Log.LastEntry);
        node.Handle(request);
        
        Assert.Equal(NodeRole.Follower, node.CurrentRole);
    }
}