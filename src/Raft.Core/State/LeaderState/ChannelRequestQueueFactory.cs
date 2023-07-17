using Raft.Core.Log;

namespace Raft.Core.State.LeaderState;

internal class ChannelRequestQueueFactory: IRequestQueueFactory
{
    private readonly ILog _log;

    public ChannelRequestQueueFactory(ILog log)
    {
        _log = log;
    }    
    
    public IRequestQueue CreateQueue()
    {
        return new ChannelRequestQueue(_log);
    }
}