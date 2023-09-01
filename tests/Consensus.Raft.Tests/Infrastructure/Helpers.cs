using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using Consensus.CommandQueue;
using Consensus.Raft.Persistence;
using Moq;

namespace Consensus.Raft.Tests.Infrastructure;

public static class Helpers
{
    public static readonly IBackgroundJobQueue NullBackgroundJobQueue = CreateNullJobQueue();
    public static readonly ITimer NullTimer = CreateNullTimer();
    public static readonly IStateMachine NullStateMachine = CreateNullStateMachine();
    public static readonly IStateMachineFactory NullStateMachineFactory = CreateNullStateMachineFactory();
    public static readonly ICommandQueue NullCommandQueue = Mock.Of<ICommandQueue>();
    public static readonly ICommandSerializer<int> NullCommandSerializer = new StubCommandSerializer<int>();
    public static readonly ITimerFactory NullTimerFactory = CreateNullTimerFactory();

    private static ITimerFactory CreateNullTimerFactory()
    {
        var mock = new Mock<ITimerFactory>();
        mock.Setup(x => x.CreateTimer())
            .Returns(NullTimer);
        return mock.Object;
    }

    private static IStateMachineFactory CreateNullStateMachineFactory()
    {
        var mock = new Mock<IStateMachineFactory>();
        return mock.Object;
    }

    private static IStateMachine CreateNullStateMachine()
    {
        return new Mock<IStateMachine>().Apply(m =>
                                         {
                                             m.Setup(x => x.Apply(It.IsAny<int>())).Returns(1);
                                         })
                                        .Object;
    }

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

    // Это будут относительные пути
    private static readonly string BaseDirectory = Path.Combine("var", "lib", "taskflux");
    private static readonly string ConsensusDirectory = Path.Combine(BaseDirectory, "consensus");

    public static (MockFileSystem Fs, IFileInfo Log, IFileInfo Metadata, IFileInfo Snapshot, IDirectoryInfo
        TemporaryDirectory) CreateFileSystem()
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