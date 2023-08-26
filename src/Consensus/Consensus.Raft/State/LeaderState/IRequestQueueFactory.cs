namespace Consensus.Raft.State.LeaderState;

public interface IRequestQueueFactory
{
    public IRequestQueue CreateQueue();
}