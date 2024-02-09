namespace TaskFlux.Consensus.Persistence;

public static class PersistenceExtensions
{
    public static void SaveSnapshot(this IPersistence facade,
                                    ISnapshot snapshot,
                                    LogEntryInfo lastIncludedEntry,
                                    CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var installer = facade.CreateSnapshot(lastIncludedEntry);
        try
        {
            foreach (var chunk in snapshot.GetAllChunks(token))
            {
                installer.InstallChunk(chunk.Span, token);
            }

            installer.Commit();
        }
        catch (Exception)
        {
            installer.Discard();
            throw;
        }
    }
}