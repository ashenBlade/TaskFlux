using Raft.Core.Node;

namespace Raft.Server.Infrastructure;

public class NullStateMachine: IStateMachine
{
    public void Submit(string command)
    { }
}