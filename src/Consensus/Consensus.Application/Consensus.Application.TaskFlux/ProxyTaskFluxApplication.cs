using Consensus.Application.TaskFlux.Serialization;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using TaskFlux.Commands;
using TaskFlux.Core;

namespace Consensus.Application.TaskFlux;

public class ProxyTaskFluxApplication : IApplication<Command, Response>
{
    private readonly IApplication _application;
    private readonly ITaskQueueSnapshotSerializer _serializer;

    public ProxyTaskFluxApplication(TaskFluxApplication application, ITaskQueueSnapshotSerializer serializer)
    {
        _application = application;
        _serializer = serializer;
    }

    public Response Apply(Command command)
    {
        return command.Apply(_application);
    }

    public void ApplyNoResponse(Command command)
    {
        command.ApplyNoResult(_application);
    }

    public ISnapshot GetSnapshot()
    {
        return new QueuesEnumeratorSnapshot(_application.TaskQueueManager, _serializer);
    }
}