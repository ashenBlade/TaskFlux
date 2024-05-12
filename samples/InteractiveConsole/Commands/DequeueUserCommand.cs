using System.Text;
using TaskFlux.Core;
using TaskFlux.Transport.Tcp.Client;
using TaskFlux.Transport.Tcp.Client.Exceptions;

namespace InteractiveConsole.Commands;

public class DequeueUserCommand : UserCommand
{
    private readonly QueueName _queue;

    public DequeueUserCommand(QueueName queue)
    {
        _queue = queue;
    }

    public override async Task Execute(ITaskFluxClient client, CancellationToken token)
    {
        try
        {
            var (key, message) = await client.DequeueAsync(_queue, token);
            Console.WriteLine($"Прочитано:");
            Console.WriteLine($" - Ключ: {key}");
            Console.WriteLine($" - Сообщение: {Encoding.UTF8.GetString(message)}");
        }
        catch (QueueEmptyException)
        {
            Console.WriteLine($"Очередь {_queue.Name} пуста");
        }
    }
}