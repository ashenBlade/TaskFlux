namespace Raft.Core.Node;

public interface IStateMachine
{
    public void Submit(string command);
}