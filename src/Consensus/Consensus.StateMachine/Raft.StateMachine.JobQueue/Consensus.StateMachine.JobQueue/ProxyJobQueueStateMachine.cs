using TaskFlux.Commands;
using TaskFlux.Core;

namespace Consensus.StateMachine.JobQueue;

public class ProxyJobQueueStateMachine: IStateMachine<Command, Result>
{
    private readonly INode _node;

    public ProxyJobQueueStateMachine(INode node)
    {
        _node = node;
    }
    
    public Result Apply(Command command)
    {
        return command.Apply(_node);
    }

    public void ApplyNoResponse(Command command)
    {
        command.ApplyNoResult(_node);
    }
}