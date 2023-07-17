using Consensus.Core.Log;

namespace Consensus.Core.State.LeaderState;

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