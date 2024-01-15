using TaskFlux.Client;
using TaskFlux.Core;

namespace InteractiveConsole.Commands;

internal class CreateQueueUserCommand : UserCommand
{
    private readonly QueueName _queueName;
    private readonly CreateQueueOptions _options;

    public CreateQueueUserCommand(QueueName queueName, CreateQueueOptions options)
    {
        _queueName = queueName;
        _options = options;
    }

    public override async Task Execute(ITaskFluxClient client, CancellationToken token)
    {
        await client.CreateQueueAsync(_queueName, _options, token);
    }
}