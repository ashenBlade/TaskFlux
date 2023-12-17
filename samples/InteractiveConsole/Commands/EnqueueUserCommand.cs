using TaskFlux.Client;
using TaskFlux.Models;

namespace InteractiveConsole.Commands;

public class EnqueueUserCommand : UserCommand
{
    private readonly QueueName _queueName;
    private readonly long _key;
    private readonly byte[] _payload;

    public EnqueueUserCommand(QueueName queueName, long key, byte[] payload)
    {
        _queueName = queueName;
        _key = key;
        _payload = payload;
    }

    public override async Task Execute(ITaskFluxClient client, CancellationToken token)
    {
        await client.EnqueueAsync(_queueName, _key, _payload, token);
        Console.WriteLine($"ОК");
    }
}