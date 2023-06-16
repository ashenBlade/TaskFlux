using Moq;
using Raft.CommandQueue;
using Raft.Core.Log;
using Raft.Core.Peer;
using Raft.Core.StateMachine;
using Serilog;

namespace Raft.Core.Tests;

public class Helpers
{
    public static readonly PeerId NodeId = new(1);
    public static readonly LogEntry LastLogEntry = new(new Term(1), 0);
    public static readonly ILogger NullLogger = new LoggerConfiguration().CreateLogger();
    public static readonly IJobQueue NullJobQueue = CreateNullJobQueue();
    public static readonly ITimer NullTimer = CreateNullTimer();

    public static readonly ICommandQueue DefaultCommandQueue = new SimpleCommandQueue();
    private static ITimer CreateNullTimer()
    {
        var mock = new Mock<ITimer>(MockBehavior.Loose);
        return mock.Object;
    }

    public static IJobQueue CreateNullJobQueue()
    {
        var mock = new Mock<IJobQueue>();
        mock.Setup(x => x.EnqueueInfinite(It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()));
        return mock.Object;
    }
    
    public static ILog CreateLog(LogEntry? logEntryInfo = null, LogEntryCheckResult result = LogEntryCheckResult.Contains, int commitIndex = 0, int lastApplied = 0)
    {
        var entry = logEntryInfo ?? LastLogEntry;
        return Mock.Of<ILog>(x => x.LastLogEntry == entry && 
                                  x.Check(It.IsAny<LogEntry>()) == result && 
                                  x.CommitIndex == commitIndex &&
                                  x.LastApplied == lastApplied);
    }
    public static INode CreateNode(Term currentTerm, PeerId? votedFor, IEnumerable<IPeer>? peers = null)
    {
        var nodeMock = new Mock<INode>(MockBehavior.Strict);
        nodeMock.SetupGet(x => x.Id).Returns(NodeId);
        nodeMock.SetupProperty(x => x.CurrentTerm, currentTerm);
        nodeMock.SetupProperty(x => x.VotedFor, votedFor);
        nodeMock.SetupProperty(x => x.PeerGroup, new PeerGroup(peers?.ToArray() ?? Array.Empty<IPeer>()));
        return nodeMock.Object;
    }

    public static RaftStateMachine CreateStateMachine(INode node, ITimer? electionTimer = null, ITimer? heartbeatTimer = null, IJobQueue? jobQueue = null, ILog? log = null, ICommandQueue? commandQueue = null)
    {
        return RaftStateMachine.Create(node, 
            NullLogger, 
            electionTimer ?? Mock.Of<ITimer>(),
            heartbeatTimer ?? Mock.Of<ITimer>(), 
            jobQueue ?? NullJobQueue,
            log ?? CreateLog(),
            commandQueue ?? DefaultCommandQueue);
    }
}