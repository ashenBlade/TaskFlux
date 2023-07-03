using Raft.Core.Node;

namespace Raft.Host.Infrastructure;

public class NullStateMachine: IStateMachine
{
    public void Submit(string command)
    { }
}