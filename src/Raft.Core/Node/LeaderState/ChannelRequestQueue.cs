using System.Threading.Channels;
using Raft.Core.Log;

namespace Raft.Core.Node.LeaderState;

internal record ChannelRequestQueue(ILog Log): IRequestQueue
{
    private readonly Channel<AppendEntriesRequestSynchronizer> _channel =
        Channel.CreateUnbounded<AppendEntriesRequestSynchronizer>();
       
    public IAsyncEnumerable<AppendEntriesRequestSynchronizer> ReadAllRequestsAsync(CancellationToken token)
    {
        return _channel.Reader.ReadAllAsync(token);
    }

    public void AddHeartbeat()
    {
        _channel.Writer.TryWrite(
            new AppendEntriesRequestSynchronizer(AlwaysTrueQuorumChecker.Instance, Log.LastLogEntryInfo.Index));
    }
}