using Consensus.Core;
using Consensus.Raft;
using TaskFlux.Commands;
using TaskFlux.Core;

namespace Consensus.Application.TaskFlux;

/// <summary>
/// Объект приложения, которое перенаправляет все запросы доменному приложению
/// </summary>
public class ProxyTaskFluxApplication : IApplication<Command, Response>
{
    private readonly TaskFluxApplication _application;

    public ProxyTaskFluxApplication(TaskFluxApplication application)
    {
        _application = application;
    }

    public Response Apply(Command command)
    {
        return command.Apply(_application);
    }

    public ISnapshot GetSnapshot()
    {
        return new ApplicationSnapshot(_application);
    }
}