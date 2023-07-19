using JobQueue.Core;
using TaskFlux.Requests;
using TaskFlux.Core;

namespace Consensus.StateMachine.JobQueue;

public class ProxyJobQueueStateMachine: IStateMachine<IRequest, IResponse>
{
    private readonly INode _node;
    private readonly ICommandBuilder _builder;

    public ProxyJobQueueStateMachine(INode node, ICommandBuilder builder)
    {
        _node = node;
        _builder = builder;
    }
    
    public IResponse Apply(IRequest request)
    {
        var command = _builder.BuildCommand(request);
        var response = command.Apply(_node);
        return response;
    }

    public void ApplyNoResponse(IRequest rawCommand)
    {
        var command = _builder.BuildCommand(rawCommand);
        command.ApplyNoResponse(_node);
    }
}