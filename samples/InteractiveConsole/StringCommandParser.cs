using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Core.Queue;
using TaskFlux.Models;
using TaskFlux.PriorityQueue;

namespace InteractiveConsole;

public static class StringCommandParser
{
    private delegate Command CommandFactory(string[] args);

    private static readonly Dictionary<string, CommandFactory> CommandToFactory = new()
    {
        {"create", GetCreateQueueCommand},
        {"delete", GetDeleteQueueCommand},
        {"enqueue", GetEnqueueCommand},
        {"dequeue", GetDequeueCommand},
        {"count", GetCountCommand},
        {"list", GetListQueuesCommand},
    };

    public static Command ParseCommand(string input)
    {
        var args = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (args.Length == 0)
        {
            throw new ArgumentException("Команда пуста");
        }

        try
        {
            return CommandToFactory[args[0].ToLower()](args);
        }
        catch (KeyNotFoundException)
        {
            throw new KeyNotFoundException($"Неизвестная команда: {args[0]}");
        }
    }

    private static CreateQueueCommand GetCreateQueueCommand(string[] args)
    {
        var queueName = QueueNameParser.Parse(args[1]);

        int? maxQueueSize = null;
        int? maxPayloadSize = null;
        (long, long)? priorityRange = null;
        var queueCode = TaskQueueBuilder.DefaultCode;

        for (var i = 2; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "WITHMAXSIZE":
                    maxQueueSize = int.Parse(args[i + 1]);
                    i++;
                    break;
                case "WITHMAXPAYLOAD":
                    maxPayloadSize = int.Parse(args[i + 1]);
                    i++;
                    break;
                case "WITHPRIORITYRANGE":
                    priorityRange = ( long.Parse(args[i + 1]), long.Parse(args[i + 2]) );
                    i += 2;
                    break;
                case "TYPE":
                    switch (args[i + 1].ToLower())
                    {
                        case "h4":
                            queueCode = PriorityQueueCode.Heap4Arity;
                            break;
                        case "qa":
                            queueCode = PriorityQueueCode.QueueArray;
                            break;
                        default:
                            throw new Exception($"Неизвестный код структуры очереди: {args[i + 1]}");
                    }

                    i++;
                    break;
                default:
                    throw new Exception($"Неизвестный аргумент: {args[i]}");
            }
        }

        return new CreateQueueCommand(queueName,
            code: queueCode,
            maxQueueSize: maxQueueSize,
            maxPayloadSize: maxPayloadSize,
            priorityRange: priorityRange);
    }

    private static DeleteQueueCommand GetDeleteQueueCommand(string[] args)
    {
        var queueName = QueueNameParser.Parse(args[1]);
        return new DeleteQueueCommand(queueName);
    }

    private static EnqueueCommand GetEnqueueCommand(string[] args)
    {
        QueueName queueName;
        long key;
        string[] data;
        if (long.TryParse(args[1], out key))
        {
            queueName = QueueName.Default;
            data = args[2..];
        }
        else
        {
            queueName = QueueNameParser.Parse(args[1]);
            key = long.Parse(args[2]);
            data = args[3..];
        }

        var payload = PayloadHelpers.Serialize(string.Join(' ', data));
        return new EnqueueCommand(key, payload, queueName);
    }

    private static DequeueCommand GetDequeueCommand(string[] args)
    {
        QueueName queueName;
        try
        {
            queueName = QueueNameParser.Parse(args[1]);
        }
        catch (IndexOutOfRangeException)
        {
            queueName = QueueName.Default;
        }

        return new DequeueCommand(queueName);
    }

    private static CountCommand GetCountCommand(string[] args)
    {
        QueueName queueName;
        try
        {
            queueName = QueueNameParser.Parse(args[1]);
        }
        catch (IndexOutOfRangeException)
        {
            queueName = QueueName.Default;
        }

        return new CountCommand(queueName);
    }

    private static ListQueuesCommand GetListQueuesCommand(string[] args)
    {
        return new ListQueuesCommand();
    }
}