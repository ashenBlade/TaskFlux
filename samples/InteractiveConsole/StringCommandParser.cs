using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Models;
using TaskQueue.Core;

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

    public static bool TryParseCommand(string input, out Command command)
    {
        var args = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (args.Length == 0)
        {
            command = null!;
            return false;
        }

        try
        {
            command = CommandToFactory[args[0].ToLower()](args);
            return true;
        }
        catch (Exception)
        {
            /* */
        }

        command = null!;
        return false;
    }

    private static CreateQueueCommand GetCreateQueueCommand(string[] args)
    {
        var queueName = QueueNameParser.Parse(args[1]);

        int? maxQueueSize = null;
        int? maxPayloadSize = null;
        (long, long)? priorityRange = null;

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
                    priorityRange = ( long.Parse(args[i + 1]), long.Parse(args[i + 1]) );
                    i += 2;
                    break;
                default:
                    throw new Exception($"Неизвестный аргумент: {args[i]}");
            }
        }

        return new CreateQueueCommand(queueName,
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