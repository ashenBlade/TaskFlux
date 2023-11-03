using Consensus.Application.TaskFlux.Serialization;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using TaskFlux.Commands;
using TaskFlux.Commands.Visitors;

namespace Consensus.Application.TaskFlux;

public class TaskFluxApplication : IApplication<Command, Result>
{
    private readonly ICommandContext _context;
    private readonly ITaskQueueSnapshotSerializer _serializer;

    public TaskFluxApplication(ICommandContext context, ITaskQueueSnapshotSerializer serializer)
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
        return new QueuesEnumeratorSnapshot(_context.Node.GetTaskQueueManager(), _serializer);
    }
}