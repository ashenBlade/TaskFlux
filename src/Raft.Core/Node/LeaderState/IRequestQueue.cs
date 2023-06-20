namespace Raft.Core.Node;

internal interface IRequestQueue
{
    public IAsyncEnumerable<AppendEntriesRequestSynchronizer> ReadAllRequestsAsync(CancellationToken token);
    public void AddHeartbeat();
}