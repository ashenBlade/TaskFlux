using Consensus.Core.Log;

namespace Consensus.Log.Tests;

public class NullSnapshotStorage : ISnapshotStorage
{
    public static readonly NullSnapshotStorage Instance = new();

    public ISnapshotFileWriter CreateTempSnapshotFile()
    {
        throw new NotImplementedException();
    }

    public LogEntryInfo? LastLogEntry => null;
}