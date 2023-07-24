namespace Consensus.Core.State.LeaderState;

public interface IRequestQueueFactory
{
    public IRequestQueue CreateQueue();
}