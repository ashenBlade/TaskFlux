namespace Raft.Core.Node;

public interface IStateMachine
{
    public void Apply(string command);
}