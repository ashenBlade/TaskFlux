using InteractiveConsole.Commands;
using TaskFlux.Client;
using TaskFlux.Core;
using TaskFlux.PriorityQueue;

namespace InteractiveConsole;

public static class StringCommandParser
{
    private delegate UserCommand CommandFactory(string[] args);

    private static readonly Dictionary<string, CommandFactory> CommandToFactory = new()
    {
        {"create", GetCreateQueueCommand},
        {"delete", GetDeleteQueueCommand},
        {"enqueue", GetEnqueueCommand},
        {"dequeue", GetDequeueCommand},
        {"count", GetCountCommand},
        {"list", GetListQueuesCommand},
    };

    public static UserCommand ParseCommand(string input)
    {
        var args = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (args.Length == 0)
        {
            throw new ArgumentException("Команда пуста");
        }

        try
        {
            var userCommand = CommandToFactory[args[0].ToLower()](args);
            userCommand = new ErrorPrinterUserCommandDecorator(userCommand);
            return userCommand;
        }
        catch (KeyNotFoundException)
        {
            throw new KeyNotFoundException($"Неизвестная команда: {args[0]}");
        }
    }

    private static UserCommand GetCreateQueueCommand(string[] args)
    {
        var queueName = QueueNameParser.Parse(args[1]);

        int? maxQueueSize = null;
        int? maxPayloadSize = null;
        (long, long)? priorityRange = null;
        PriorityQueueCode? queueCode = null;

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

        var options = new CreateQueueOptions();
        switch (queueCode)
        {
            case null:
                break;
            case PriorityQueueCode.Heap4Arity:
                options = options.UseHeap();
                break;
            case PriorityQueueCode.QueueArray:
                if (priorityRange is var (min, max))
                {
                    options = options.UseQueueArray(min, max);
                    priorityRange = null; // Чтобы дальше не использовали
                }
                else
                {
                    throw new Exception("Для реализации QueueArray необходимо указать диапазон допустимых ключей");
                }

                break;
        }

        if (priorityRange is var (minKey, maxKey))
        {
            options = options.WithPriorityRange(minKey, maxKey);
        }

        if (maxQueueSize is { } mqs)
        {
            options = options.WithMaxQueueSize(mqs);
        }

        if (maxPayloadSize is { } mps)
        {
            options = options.WithMaxMessageSize(mps);
        }

        return new CreateQueueUserCommand(queueName, options);
    }

    private static DeleteQueueUserCommand GetDeleteQueueCommand(string[] args)
    {
        var queueName = QueueNameParser.Parse(args[1]);
        return new DeleteQueueUserCommand(queueName);
    }

    private static EnqueueUserCommand GetEnqueueCommand(string[] args)
    {
        QueueName queueName;
        string[] data;
        if (long.TryParse(args[1], out var key))
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
        return new EnqueueUserCommand(queueName, key, payload);
    }

    private static DequeueUserCommand GetDequeueCommand(string[] args)
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

        return new DequeueUserCommand(queueName);
    }

    private static GetCountUserCommand GetCountCommand(string[] args)
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

        return new GetCountUserCommand(queueName);
    }

    private static ListQueuesUserCommand GetListQueuesCommand(string[] args)
    {
        return new ListQueuesUserCommand();
    }
}