namespace Consensus.Raft.State.LeaderState;

public interface ISynchronizer
{
    public void NotifySuccess();
    public void NotifyGreaterTerm(Term greaterTerm);
    public void TryWaitGreaterTerm(out Term greaterTerm);
}