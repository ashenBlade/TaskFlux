using Castle.Core.Logging;
using Moq;
using Raft.Core.Commands;
using Raft.Core.StateMachine;

namespace Raft.Core.Tests;

public class FollowerStateTests
{
    private static FollowerState CreateState(IStateMachine stateMachine) => new(stateMachine, Infrastructure.NullLogger);
    private static readonly PeerId NodeId = new(1);
    private static INode CreateNode(Term currentTerm, PeerId? votedFor, LogEntryInfo lastLogEntry)
    {
        var nodeMock = new Mock<INode>(MockBehavior.Strict);
        nodeMock.SetupGet(x => x.Id).Returns(NodeId);
        nodeMock.SetupProperty(x => x.CurrentTerm, currentTerm);
        nodeMock.SetupProperty(x => x.VotedFor, votedFor);
        nodeMock.SetupProperty(x => x.LastLogEntry, lastLogEntry);
        return nodeMock.Object;
    }

    private static RaftStateMachine CreateStateMachine(INode node, ITimer? electionTimer = null, IJobQueue? jobQueue = null)
    {
        return RaftStateMachine.Start(node, 
            Infrastructure.NullLogger, 
            electionTimer ?? Mock.Of<ITimer>(),
            Mock.Of<ITimer>(), 
            jobQueue ?? Infrastructure.NullJobQueue);
    }

    [Fact]
    public void ПриСоздании__ПервоеСостояниеДолжноБыть__Follower()
    {
        var machine = RaftStateMachine.Start(CreateNode(new(1), null, new()), Infrastructure.NullLogger, Infrastructure.NullTimer, Infrastructure.NullTimer, Infrastructure.NullJobQueue);
        Assert.Equal(NodeRole.Follower, machine.CurrentRole);
    }
    
    [Fact]
    public async Task ПриЗапросеRequestVoteСБолееВысокимТермом__КогдаПреждеНеГолосовал__ДолженВыставитьСвойТермВБолееВысокий()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null, new LogEntryInfo(oldTerm, 0));
        var stateMachine = CreateStateMachine(node);
        var state = CreateState(stateMachine);
        
        var expectedTerm = oldTerm.Increment();
        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = expectedTerm, 
            LastLog = new LogEntryInfo(oldTerm, 0)
        };

        await state.Apply(request);
        
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }
    
    [Fact]
    public async Task ПриЗапросеRequestVote__СБолееВысокимТермом__ДолженОтветитьПоложительно()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null, new LogEntryInfo(oldTerm, 0));
        var stateMachine = CreateStateMachine(node);
        
        var state = CreateState(stateMachine);
        var expectedTerm = oldTerm.Increment();
        
        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = expectedTerm, 
            LastLog = new LogEntryInfo(oldTerm, 0)
        };

        var response = await state.Apply(request);
        
        Assert.True(response.VoteGranted);
    }
    
    
    [Theory]
    [InlineData(2, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(3, 1)]
    [InlineData(3, 3)]
    public async Task ПриЗапросеRequestVote__СТермомНеБольшеСвоего__ДолженОтветитьОтрицательно(int myTerm, int otherTerm)
    {
        var oldTerm = new Term(myTerm);
        
        var node = CreateNode(oldTerm, null, new LogEntryInfo(oldTerm, 0));
        var stateMachine = CreateStateMachine(node);
        var state = CreateState(stateMachine);
        
        var expectedTerm = new Term(otherTerm);
        var request = new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = expectedTerm, 
            LastLog = new LogEntryInfo(oldTerm, 0)
        };

        var response = await state.Apply(request);
        
        Assert.False(response.VoteGranted);
    }
    
    [Fact]
    public async Task ПриЗапросеRequestVote__ДолженПерезапуститьElectionTimeout()
    {
        var node = CreateNode(new Term(1), null, new LogEntryInfo(new Term(1), 0));
        var timer = new Mock<ITimer>();
        timer.Setup(x => x.Reset())
             .Verifiable();
        var stateMachine = CreateStateMachine(node, electionTimer: timer.Object);
        var state = CreateState(stateMachine);

        await state.Apply(new RequestVoteRequest()
        {
            CandidateId = new PeerId(2), 
            CandidateTerm = new Term(1),
            LastLog = new LogEntryInfo(new(1), 0)
        });
        
        timer.Verify(x => x.Reset(), Times.Once());
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ДолженПерейтиВСостояниеCandidate()
    {
        var node = CreateNode(new Term(1), null, new LogEntryInfo(new Term(1), 0));
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
        var node = CreateNode(oldTerm, null, new LogEntryInfo(new Term(1), 0));
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        var stateMachine = CreateStateMachine(node, timer.Object);

        timer.Raise(x => x.Timeout += null);

        var expectedTerm = oldTerm.Increment();
        Assert.Equal(expectedTerm, node.CurrentTerm);
    }

    [Fact]
    public void ПриСрабатыванииElectionTimeout__ПослеСрабатыванияОбработчика__ДолженПроголосоватьЗаСебя()
    {
        var oldTerm = new Term(1);
        var node = CreateNode(oldTerm, null, new LogEntryInfo(new Term(1), 0));
        var timer = new Mock<ITimer>(MockBehavior.Loose);
        using var stateMachine = CreateStateMachine(node, timer.Object);

        timer.Raise(x => x.Timeout += null);

        Assert.Equal(node.Id, node.VotedFor);
    }
}