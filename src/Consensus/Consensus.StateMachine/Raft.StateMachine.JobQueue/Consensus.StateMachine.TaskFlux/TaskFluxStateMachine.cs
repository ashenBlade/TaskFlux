using Consensus.Raft;
using Consensus.Raft.Persistence;
using Consensus.StateMachine.TaskFlux.Serialization;
using TaskFlux.Commands;
using TaskFlux.Commands.Visitors;

namespace Consensus.StateMachine.TaskFlux;

public class TaskFluxStateMachine : IStateMachine<Command, Result>
{
    private readonly ICommandContext _context;
    private readonly IJobQueueSnapshotSerializer _serializer;

    public TaskFluxStateMachine(ICommandContext context, IJobQueueSnapshotSerializer serializer)
    {
        _context = context;
        _serializer = serializer;
    }

    public Result Apply(Command command)
    {
        return command.Apply(_context);
    }

    public void ApplyNoResponse(Command command)
    {
        command.ApplyNoResult(_context);
    }

    public ISnapshot GetSnapshot()
    {
        return new QueuesEnumeratorSnapshot(_context.Node.GetJobQueueManager(), _serializer);
    }
}