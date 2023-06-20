namespace Raft.Core.Node;

internal class ChannelRequestQueueFactory: IRequestQueueFactory
{
    public static readonly ChannelRequestQueueFactory Instance = new();
    public IRequestQueue CreateQueue()
    {
        return new ChannelRequestQueue();
    }
}