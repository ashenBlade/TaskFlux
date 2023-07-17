namespace Consensus.Core.State.LeaderState;

internal interface IRequestQueueFactory
{
    public IRequestQueue CreateQueue();
}