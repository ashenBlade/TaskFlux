using TaskFlux.Commands;
using TaskFlux.Core;

namespace Consensus.StateMachine.JobQueue;

public class ProxyJobQueueStateMachine: IStateMachine<Command, Result>
{
    private readonly ICommandContext _context;

    public ProxyJobQueueStateMachine(ICommandContext context)
    {
        _context = context;
    }
    
    public Result Apply(Command command)
    {
        return command.Apply(_context);
    }

    public void ApplyNoResponse(Command command)
    {
        command.ApplyNoResult(_context);
    }
}