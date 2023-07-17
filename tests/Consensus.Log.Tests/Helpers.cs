using Moq;

namespace Consensus.Log.Tests;

public static class Helpers
{
    public static readonly ILogStorage NullStorage = CreateNullStorage();

    private static ILogStorage CreateNullStorage()
    {
        return new Mock<ILogStorage>(MockBehavior.Loose).Object;
    }
}