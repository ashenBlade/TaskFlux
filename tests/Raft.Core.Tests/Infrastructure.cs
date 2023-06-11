using Moq;
using Serilog;

namespace Raft.Core.Tests;

public class Infrastructure
{
    public static readonly ILogger NullLogger = new LoggerConfiguration().CreateLogger();
    public static readonly IJobQueue NullJobQueue = CreateNullJobQueue();
    public static readonly ITimer NullTimer = CreateNullTimer();

    private static ITimer CreateNullTimer()
    {
        var mock = new Mock<ITimer>(MockBehavior.Loose);
        return mock.Object;
    }

    private static IJobQueue CreateNullJobQueue()
    {
        var mock = new Mock<IJobQueue>();
        mock.Setup(x => x.EnqueueInfinite(It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()));
        return mock.Object;
    }
}