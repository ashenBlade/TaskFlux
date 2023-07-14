using Moq;
using Raft.Core.Log;

namespace Raft.Log.Tests;

public static class Helpers
{
    public static readonly ILogStorage NullStorage = CreateNullStorage();

    private static ILogStorage CreateNullStorage()
    {
        return new Mock<ILogStorage>(MockBehavior.Loose).Object;
    }
}