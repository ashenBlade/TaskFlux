using Moq;
using TaskFlux.Consensus.Tests.Stubs;

namespace TaskFlux.Consensus.Tests.Infrastructure;

public static class Helpers
{
    public static readonly IBackgroundJobQueue NullBackgroundJobQueue = CreateNullJobQueue();
    public static readonly ITimer NullTimer = CreateNullTimer();
    public static readonly IApplication NullApplication = CreateNullApplication();
    public static readonly IDeltaExtractor<int> NullDeltaExtractor = new StubDeltaExtractor<int>();
    public static readonly ITimerFactory NullTimerFactory = CreateNullTimerFactory();
    public static readonly IApplicationFactory NullApplicationFactory = CreateNullApplicationFactory();

    private static IApplicationFactory CreateNullApplicationFactory()
    {
        return new Mock<IApplicationFactory>().Apply(f =>
                                               {
                                                   f.Setup(x => x.CreateSnapshot(It.IsAny<ISnapshot?>(),
                                                         It.IsAny<IEnumerable<byte[]>>()))
                                                    .Returns(new StubSnapshot(Array.Empty<byte>()));
                                                   f.Setup(x => x.Restore(It.IsAny<ISnapshot?>(),
                                                         It.IsAny<IEnumerable<byte[]>>()))
                                                    .Returns(CreateNullApplication());
                                               })
                                              .Object;
    }

    private static ITimerFactory CreateNullTimerFactory()
    {
        var mock = new Mock<ITimerFactory>();
        mock.Setup(x => x.CreateHeartbeatTimer())
            .Returns(NullTimer);
        mock.Setup(x => x.CreateElectionTimer())
            .Returns(NullTimer);
        return mock.Object;
    }

    private static IApplication CreateNullApplication()
    {
        return new Mock<IApplication>().Apply(m =>
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
        mock.Setup(x => x.Accept(It.IsAny<IBackgroundJob>(), It.IsAny<CancellationToken>()));
        return mock.Object;
    }
}