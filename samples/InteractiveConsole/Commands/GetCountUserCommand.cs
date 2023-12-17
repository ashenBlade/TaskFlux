using TaskFlux.Client;
using TaskFlux.Models;

namespace InteractiveConsole.Commands;

public class GetCountUserCommand : UserCommand
{
    private readonly QueueName _queueName;

    public GetCountUserCommand(QueueName queueName)
    {
        _queueName = queueName;
    }

    public override async Task Execute(ITaskFluxClient client, CancellationToken token)
    {
        var count = await client.GetQueueLengthAsync(_queueName, token);
        Console.WriteLine($"Размер очереди: {count}");
    }
}