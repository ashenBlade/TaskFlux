using Consensus.Core.Log;
using Consensus.Persistence;

namespace Consensus.Persistence.Tests;

public class NullSnapshotStorage : ISnapshotStorage
{
    public static readonly NullSnapshotStorage Instance = new();

    public ISnapshotFileWriter CreateTempSnapshotFile()
    {
        throw new NotImplementedException();
    }

    public LogEntryInfo? LastLogEntry => null;
}