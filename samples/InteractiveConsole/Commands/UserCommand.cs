using TaskFlux.Client;

namespace InteractiveConsole.Commands;

public abstract class UserCommand
{
    public abstract Task Execute(ITaskFluxClient client, CancellationToken token);
}