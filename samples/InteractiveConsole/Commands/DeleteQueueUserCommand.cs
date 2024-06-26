using TaskFlux.Core;
using TaskFlux.Transport.Tcp.Client;
using TaskFlux.Transport.Tcp.Client.Exceptions;

namespace InteractiveConsole.Commands;

public class DeleteQueueUserCommand : UserCommand
{
    private readonly QueueName _queueName;

    public DeleteQueueUserCommand(QueueName queueName)
    {
        _queueName = queueName;
    }

    public override async Task Execute(ITaskFluxClient client, CancellationToken token)
    {
        try
        {
            await client.DeleteQueueAsync(_queueName, token);
        }
        catch (QueueNotExistException)
        {
            Console.WriteLine($"Очередь {_queueName} не существует");
        }
    }
}