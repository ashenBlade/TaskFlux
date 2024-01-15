namespace TaskFlux.Consensus.Persistence.Snapshot;

public static class FileSystemSnapshotStorageExtensions
{
    public static bool TryGetSnapshot(this FileSystemSnapshotStorage storage, out ISnapshot snapshot)
    {
        if (storage.LastLogEntry.IsTomb)
        {
            snapshot = null!;
            return false;
        }

        snapshot = storage.GetSnapshot();
        return true;
    }
}