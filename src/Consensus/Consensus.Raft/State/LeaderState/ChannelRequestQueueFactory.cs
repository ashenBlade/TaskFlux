using Consensus.Raft.Persistence;

namespace Consensus.Raft.State.LeaderState;

public class ChannelRequestQueueFactory : IRequestQueueFactory
{
    private readonly IPersistenceFacade _persistenceFacade;

    public ChannelRequestQueueFactory(IPersistenceFacade persistenceFacade)
    {
        _persistenceFacade = persistenceFacade;
    }

    public IRequestQueue CreateQueue()
    {
        return new ChannelRequestQueue(_persistenceFacade);
    }
}