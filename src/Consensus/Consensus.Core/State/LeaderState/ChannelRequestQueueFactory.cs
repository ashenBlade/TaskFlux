using Consensus.Core.Log;

namespace Consensus.Core.State.LeaderState;

public class ChannelRequestQueueFactory : IRequestQueueFactory
{
    private readonly IPersistenceManager _persistenceManager;

    public ChannelRequestQueueFactory(IPersistenceManager persistenceManager)
    {
        _persistenceManager = persistenceManager;
    }

    public IRequestQueue CreateQueue()
    {
        return new ChannelRequestQueue(_persistenceManager);
    }
}