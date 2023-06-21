using Moq;
using Raft.CommandQueue;
using Raft.Core.Log;
using Raft.Core.Node;
using Serilog;

namespace Raft.Core.Tests;

public class Helpers
{
    public static readonly NodeId NodeId = new(1);
    public static readonly LogEntryInfo LastLogEntryInfo = new(new Term(1), 0);
    public static readonly ILogger NullLogger = new LoggerConfiguration().CreateLogger();
    public static readonly IJobQueue NullJobQueue = CreateNullJobQueue();
    public static readonly ITimer NullTimer = CreateNullTimer();
    public static readonly IStateMachine NullStateMachine = CreateNullStateMachine();

    private static IStateMachine CreateNullStateMachine()
    {
        return new Mock<IStateMachine>().Apply(m =>
        {
            m.Setup(x => x.Submit(It.IsAny<string>()));
        }).Object;
    }

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
    
    public static ILog CreateLog(LogEntryInfo? logEntryInfo = null, LogEntryCheckResult result = LogEntryCheckResult.Contains, int commitIndex = 0, int lastApplied = 0)
    {
        var entry = logEntryInfo ?? LastLogEntryInfo;
        return Mock.Of<ILog>(x => x.LastLogEntryInfo == entry && 
                                  x.Check(It.IsAny<LogEntryInfo>()) == result && 
                                  x.CommitIndex == commitIndex &&
                                  x.LastApplied == lastApplied);
    }

    public static RaftNode CreateStateMachine(Term currentTerm, NodeId? votedFor, IEnumerable<IPeer>? peers = null, ITimer? electionTimer = null, ITimer? heartbeatTimer = null, IJobQueue? jobQueue = null, ILog? log = null, ICommandQueue? commandQueue = null)
    {
        return RaftNode.Create(NodeId, 
            new PeerGroup(peers?.ToArray() ?? Array.Empty<IPeer>()),
            votedFor,
            currentTerm,
            NullLogger, 
            electionTimer ?? Mock.Of<ITimer>(),
            heartbeatTimer ?? Mock.Of<ITimer>(), 
            jobQueue ?? NullJobQueue,
            log ?? CreateLog(),
            commandQueue ?? DefaultCommandQueue,
            NullStateMachine);
    }
}