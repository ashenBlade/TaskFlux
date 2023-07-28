using System.Runtime.CompilerServices;
using Consensus.CommandQueue;
using Moq;
using Consensus.Core.Log;
using Consensus.Core.State.LeaderState;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Core.Tests;

public static class Helpers
{
    public static readonly NodeId NodeId = new(1);
    public static readonly LogEntryInfo LastLogEntryInfo = new(new Term(1), 0);
    public static readonly ILogger NullLogger = new LoggerConfiguration().CreateLogger();
    public static readonly IBackgroundJobQueue NullBackgroundJobQueue = CreateNullJobQueue();
    public static readonly ITimer NullTimer = CreateNullTimer();
    public static readonly IStateMachine NullStateMachine = CreateNullStateMachine();
    public static readonly IMetadataStorage NullMetadataStorage = CreateNullStorage();
    public static readonly IRequestQueueFactory NullRequestQueueFactory = CreateNullRequestQueueFactory();
    
    private static IRequestQueueFactory CreateNullRequestQueueFactory()
    {
        var mockQueueFactory = new Mock<IRequestQueueFactory>();
        var mockQueue = new Mock<IRequestQueue>();
        mockQueue.Setup(x => x.AddAppendEntries(It.IsAny<AppendEntriesRequestSynchronizer>()));
        mockQueue.Setup(x => x.AddHeartbeatIfEmpty());
        mockQueue.Setup(x => x.ReadAllRequestsAsync(It.IsAny<CancellationToken>()))
                 .Returns(CreateNullEnumerable);

        mockQueueFactory.Setup(x => x.CreateQueue()).Returns(mockQueue.Object);
        return mockQueueFactory.Object;

        async IAsyncEnumerable<AppendEntriesRequestSynchronizer> CreateNullEnumerable([EnumeratorCancellation] CancellationToken token)
        {
            yield break;
        }

    }

    private static IMetadataStorage CreateNullStorage()
    {
        return new StubMetadataStorage(Term.Start, null);
    }

    private static IStateMachine CreateNullStateMachine()
    {
        return new Mock<IStateMachine>().Apply(m =>
        {
            m.Setup(x => x.Apply(It.IsAny<int>())).Returns(1);
        }).Object;
    }

    public static readonly ICommandQueue DefaultCommandQueue = new SimpleCommandQueue();
    private static ITimer CreateNullTimer()
    {
        var mock = new Mock<ITimer>(MockBehavior.Loose);
        return mock.Object;
    }

    public static IBackgroundJobQueue CreateNullJobQueue()
    {
        var mock = new Mock<IBackgroundJobQueue>();
        mock.Setup(x => x.EnqueueInfinite(It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()));
        return mock.Object;
    }
    
    public static ILog CreateLog(LogEntryInfo? logEntryInfo = null, int commitIndex = 0, int lastApplied = 0)
    {
        var entry = logEntryInfo ?? LastLogEntryInfo;
        return Mock.Of<ILog>(x => x.LastEntry == entry && 
                                  x.CommitIndex == commitIndex &&
                                  x.LastApplied == lastApplied &&
                                  x.Contains(It.IsAny<LogEntryInfo>()) == true);
    }

    public static RaftConsensusModule<int, int> CreateNode(Term currentTerm, NodeId? votedFor, IEnumerable<IPeer>? peers = null, ITimer? electionTimer = null, ITimer? heartbeatTimer = null, IBackgroundJobQueue? jobQueue = null, ILog? log = null, ICommandQueue? commandQueue = null)
    {
        return RaftConsensusModule<int, int>.Create(
            NodeId, 
            new PeerGroup(peers?.ToArray() ?? Array.Empty<IPeer>()),
            NullLogger, 
            electionTimer ?? Mock.Of<ITimer>(),
            heartbeatTimer ?? Mock.Of<ITimer>(), 
            jobQueue ?? NullBackgroundJobQueue,
            log ?? CreateLog(),
            commandQueue ?? DefaultCommandQueue,
            NullStateMachine,
            new StubMetadataStorage(currentTerm, votedFor),
            new StubSerializer<int>()
            {
                Deserialized = 1,
                Serialized = Array.Empty<byte>()
            }, NullRequestQueueFactory);
    }
}