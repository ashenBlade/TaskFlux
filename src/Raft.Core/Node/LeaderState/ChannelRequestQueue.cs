using System.Threading.Channels;
using Raft.Core.Log;

namespace Raft.Core.Node;

internal record ChannelRequestQueue: IRequestQueue
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
            new AppendEntriesRequestSynchronizer(AlwaysTrueQuorumChecker.Instance,
            Array.Empty<LogEntry>()));
    }
}