using System.ComponentModel;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Subscription;
using TaskFlux.PriorityQueue;
using TaskFlux.PriorityQueue.Heap;
using TaskFlux.PriorityQueue.QueueArray;

namespace TaskFlux.Core.Queue;

public class TaskQueueBuilder : ITaskQueueBuilder
{
    /// <summary>
    /// Фабрика для создания ждунов
    /// </summary>
    private readonly IQueueSubscriberManagerFactory _queueSubscriberManagerFactory;

    /// <summary>
    /// Название для очереди, которое нужно использовать
    /// </summary>
    private QueueName _name;

    /// <summary>
    /// Реализация приоритетной очереди
    /// </summary>
    private PriorityQueueCode _queueCode;

    /// <summary>
    /// Изначальные данные, которые нужно записать в очередь изнчально
    /// </summary>
    private IEnumerable<QueueRecord>? _payload;

    private int? _maxSize;
    private (long Min, long Max)? _priorityRange;
    private int? _maxPayloadSize;

    public TaskQueueBuilder(QueueName name,
                            PriorityQueueCode code,
                            IQueueSubscriberManagerFactory queueSubscriberManagerFactory)
    {
        _name = name;
        _queueCode = code;
        _queueSubscriberManagerFactory = queueSubscriberManagerFactory;
    }

    public ITaskQueueBuilder WithQueueName(QueueName name)
    {
        _name = name;
        return this;
    }

    public ITaskQueueBuilder WithData(IReadOnlyCollection<QueueRecord> data)
    {
        ArgumentNullException.ThrowIfNull(data);

        _payload = data;
        return this;
    }

    public ITaskQueueBuilder WithMaxQueueSize(int? maxSize)
    {
        if (maxSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxSize), maxSize,
                "Максимальный размер очереди не может быть отрицательным");
        }

        _maxSize = maxSize;
        return this;
    }


    public ITaskQueueBuilder WithPriorityRange(long min, long max)
    {
        if (max < min)
        {
            throw new ArgumentOutOfRangeException(nameof(min), min,
                $"Наименьшее значение приоритета не может быть больше максимального. Получен диапазон: {min} - {max}");
        }

        _priorityRange = ( min, max );
        return this;
    }

    public ITaskQueueBuilder WithQueueImplementation(PriorityQueueCode implementation)
    {
        if (!Enum.IsDefined(implementation))
        {
            throw new InvalidEnumArgumentException(nameof(implementation), ( int ) implementation,
                typeof(PriorityQueueCode));
        }

        _queueCode = implementation;
        return this;
    }

    public ITaskQueueBuilder WithMaxPayloadSize(int? maxPayloadSize)
    {
        if (maxPayloadSize is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPayloadSize), maxPayloadSize,
                "Максимальный размер сообщения не может быть отрицательным");
        }

        _maxPayloadSize = maxPayloadSize;
        return this;
    }

    /// <summary>
    /// Создать очередь с указанными параметрами
    /// </summary>
    /// <returns>Созданная очередь</returns>
    /// <exception cref="InvalidOperationException">Указанный набор параметров представляет неправильную комбинацию</exception>
    public ITaskQueue Build()
    {
        var policies = BuildPolicies();
        var queue = BuildPriorityQueue();

        if (_payload is { } payload)
        {
            FillPriorityQueue(payload, queue);
        }

        return new TaskQueue(_name, queue, policies, _queueSubscriberManagerFactory.CreateQueueSubscriberManager());
    }

    private IPriorityQueue BuildPriorityQueue()
    {
        switch (_queueCode)
        {
            case PriorityQueueCode.Heap4Arity:
                return new HeapPriorityQueue();
            case PriorityQueueCode.QueueArray:
                if (_priorityRange is var (min, max))
                {
                    return new QueueArrayPriorityQueue(min, max);
                }

                throw new InvalidOperationException("Диапазон ключей для структуры списка очередей не указан");
        }

        throw new InvalidEnumArgumentException(nameof(_queueCode), ( int ) _queueCode, typeof(PriorityQueueCode));
    }

    private void FillPriorityQueue(IEnumerable<QueueRecord> payload, IPriorityQueue queue)
    {
        foreach (var (key, message) in payload)
        {
            queue.Enqueue(key, message);
        }
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

    /// <summary>
    /// Создать очередь по умолчанию с нужными выставленными параметрами
    /// </summary>
    /// <returns>Очередь по умолчанию</returns>
    public ITaskQueue CreateDefault()
    {
        return new TaskQueue(QueueName.Default, new HeapPriorityQueue(), Array.Empty<QueuePolicy>(),
            _queueSubscriberManagerFactory.CreateQueueSubscriberManager());
    }
}