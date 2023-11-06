using TaskFlux.Core.Policies;
using TaskFlux.Models;
using TaskFlux.PriorityQueue.StandardLibrary;

namespace TaskFlux.Core.Queue;

public class TaskQueueBuilder
{
    private QueueName? _name;
    private IReadOnlyCollection<(long Key, byte[] Value)>? _payload;

    private int? _maxSize;
    private (long Min, long Max)? _priorityRange;
    private int? _maxPayloadSize;

    public TaskQueueBuilder(QueueName name)
    {
        _name = name;
    }

    public TaskQueueBuilder WithQueueName(QueueName name)
    {
        _name = name;
        return this;
    }

    public TaskQueueBuilder WithData(IReadOnlyCollection<(long, byte[])> data)
    {
        _payload = data;
        return this;
    }

    public TaskQueueBuilder WithMaxQueueSize(int maxSize)
    {
        if (maxSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxSize), maxSize,
                "Максимальный размер очереди не может быть отрицательным");
        }

        _maxSize = maxSize;
        return this;
    }

    public TaskQueueBuilder WithPriorityRange(long min, long max)
    {
        if (max < min)
        {
            throw new ArgumentOutOfRangeException(nameof(min), min,
                $"Наименьшее значение приоритета не может быть больше максимального. Получен диапазон: {min} - {max}");
        }

        _priorityRange = ( min, max );
        return this;
    }

    public TaskQueueBuilder WithMaxPayloadSize(int maxPayloadSize)
    {
        _maxPayloadSize = maxPayloadSize;
        return this;
    }

    public ITaskQueue Build()
    {
        var name = BuildQueueName();
        var policies = BuildPolicies();
        var priorityQueue = BuildPriorityQueue();

        var queue = new TaskQueue(name, priorityQueue, policies);
        return queue;
    }

    private QueueName BuildQueueName()
    {
        return _name ?? throw new InvalidOperationException("Название очереди не проставлено");
    }

    private StandardLibraryPriorityQueue BuildPriorityQueue()
    {
        var priorityQueue = new StandardLibraryPriorityQueue();

        if (_payload is {Count: > 0} payload)
        {
            foreach (var (key, value) in payload)
            {
                priorityQueue.Enqueue(key, value);
            }
        }

        return priorityQueue;
    }

    private QueuePolicy[] BuildPolicies()
    {
        var result = new List<QueuePolicy>();

        if (_maxSize is { } maxSize)
        {
            result.Add(new MaxQueueSizeQueuePolicy(maxSize));
        }

        if (_priorityRange is var (min, max))
        {
            result.Add(new PriorityRangeQueuePolicy(min, max));
        }

        if (_maxPayloadSize is { } maxPayloadSize)
        {
            result.Add(new MaxPayloadSizeQueuePolicy(maxPayloadSize));
        }

        return result.ToArray();
    }
}