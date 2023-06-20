namespace Raft.Core.Node;

internal interface IRequestQueueFactory
{
    public IRequestQueue CreateQueue();
}