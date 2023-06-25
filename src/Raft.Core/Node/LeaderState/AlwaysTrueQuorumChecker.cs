namespace Raft.Core.Node.LeaderState;

public class AlwaysTrueQuorumChecker: IQuorumChecker
{
    public static readonly AlwaysTrueQuorumChecker Instance = new();
    public bool IsQuorumReached(int votes)
    {
        return true;
    }
}