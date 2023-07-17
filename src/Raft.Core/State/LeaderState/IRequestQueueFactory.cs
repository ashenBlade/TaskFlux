namespace Raft.Core.State.LeaderState;

internal interface IRequestQueueFactory
{
    public IRequestQueue CreateQueue();
}