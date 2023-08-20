using TaskFlux.Commands;

namespace Consensus.StateMachine.TaskFlux;

public class TaskFluxStateMachine : IStateMachine<Command, Result>
{
    private readonly ICommandContext _context;

    public TaskFluxStateMachine(ICommandContext context)
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