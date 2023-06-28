namespace Raft.Core.Node.LeaderState;

internal interface IRequestQueueFactory
{
    public IRequestQueue CreateQueue();
}