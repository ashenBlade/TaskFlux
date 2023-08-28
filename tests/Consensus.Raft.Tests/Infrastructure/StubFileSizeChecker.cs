using Consensus.Raft.Persistence.LogFileCheckStrategy;

namespace Consensus.Raft.Tests.Infrastructure;

public class StubFileSizeChecker : ILogFileSizeChecker
{
    public static StubFileSizeChecker Exceeded => new(true);
    public static StubFileSizeChecker False => new(false);

    private readonly bool _exceeded;

    public StubFileSizeChecker(bool exceeded)
    {
        _exceeded = exceeded;
    }

    public bool IsLogFileSizeExceeded(ulong logFileSize)
    {
        return _exceeded;
    }
}