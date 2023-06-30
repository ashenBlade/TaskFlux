using System.Threading.Channels;
using Raft.Core.Log;

namespace Raft.Core.Node.LeaderState;

internal record ChannelRequestQueue(ILog Log): IRequestQueue
{
    private readonly Channel<AppendEntriesRequestSynchronizer> _channel =
        Channel.CreateUnbounded<AppendEntriesRequestSynchronizer>(new UnboundedChannelOptions()
        {
            SingleReader = true,
            SingleWriter = false,
        });
       
    public IAsyncEnumerable<AppendEntriesRequestSynchronizer> ReadAllRequestsAsync(CancellationToken token)
    {
        return _channel.Reader.ReadAllAsync(token);
    }

    public void AddHeartbeat()
    {
        _channel.Writer.TryWrite(
            new AppendEntriesRequestSynchronizer(AlwaysTrueQuorumChecker.Instance, Log.LastEntry.Index));
    }

    public void AddAppendEntries(AppendEntriesRequestSynchronizer synchronizer)
    {
        _channel.Writer.TryWrite(synchronizer);
    }
}