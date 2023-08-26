using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using System.Runtime.CompilerServices;
using Consensus.CommandQueue;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Metadata;
using Consensus.Raft.State.LeaderState;
using Moq;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Raft.Tests.Infrastructure;

public static class Helpers
{
    public static readonly NodeId NodeId = new(1);
    public static readonly LogEntryInfo LastLogEntryInfo = new(new Term(1), 0);
    public static readonly ILogger NullLogger = new LoggerConfiguration().CreateLogger();
    public static readonly IBackgroundJobQueue NullBackgroundJobQueue = CreateNullJobQueue();
    public static readonly ITimer NullTimer = CreateNullTimer();
    public static readonly IStateMachine NullStateMachine = CreateNullStateMachine();
    public static readonly IStateMachineFactory NullStateMachineFactory = CreateNullStateMachineFactory();
    public static readonly ICommandQueue NullCommandQueue = Mock.Of<ICommandQueue>();
    public static readonly ICommandSerializer<int> NullCommandSerializer = new StubCommandSerializer<int>();

    private static IStateMachineFactory CreateNullStateMachineFactory()
    {
        var mock = new Mock<IStateMachineFactory>();
        return mock.Object;
    }

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

        async IAsyncEnumerable<AppendEntriesRequestSynchronizer> CreateNullEnumerable(
            [EnumeratorCancellation] CancellationToken token)
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
                                         })
                                        .Object;
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
        mock.Setup(x => x.RunInfinite(It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()));
        return mock.Object;
    }

    public static IPersistenceFacade CreateLog(LogEntryInfo? logEntryInfo = null,
                                                int commitIndex = 0,
                                                int lastApplied = 0)
    {
        var entry = logEntryInfo ?? LastLogEntryInfo;
        return Mock.Of<IPersistenceFacade>(x =>
            x.LastEntry == entry
         && x.CommitIndex == commitIndex
         && x.LastAppliedIndex == lastApplied
         && x.Contains(It.IsAny<LogEntryInfo>()) == true);
    }
    
    // Это будут относительные пути
    private static readonly string BaseDirectory = Path.Combine("var", "lib", "taskflux");
    private static readonly string ConsensusDirectory = Path.Combine(BaseDirectory, "consensus");
    
    public static (MockFileSystem Fs, IFileInfo Log, IFileInfo Metadata, IFileInfo Snapshot, IDirectoryInfo TemporaryDirectory) CreateFileSystem()
    {
        var fs = new MockFileSystem(new Dictionary<string, MockFileData>()
        {
            [ConsensusDirectory] = new MockDirectoryData()    
        });

        var log = fs.FileInfo.New(Path.Combine(ConsensusDirectory, Constants.LogFileName));
        var metadata = fs.FileInfo.New(Path.Combine(ConsensusDirectory, Constants.MetadataFileName));
        var snapshot = fs.FileInfo.New(Path.Combine(ConsensusDirectory, Constants.SnapshotFileName));
        var tempDirectory = fs.DirectoryInfo.New(Path.Combine(ConsensusDirectory, "temporary"));

        log.Create();
        metadata.Create();
        snapshot.Create();
        tempDirectory.Create();

        return ( fs, log, metadata, snapshot, tempDirectory );
    }
}