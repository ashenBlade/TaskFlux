using System.ComponentModel;
using System.Diagnostics;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Subscription;
using TaskFlux.Domain;
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
    /// Последний известный Id записи
    /// </summary>
    private RecordId _lastRecordId;

    /// <summary>
    /// Изначальные данные, которые нужно записать в очередь изнчально
    /// </summary>
    private IEnumerable<QueueRecord>? _payload;

    /// <summary>
    /// Максимальный размер очереди
    /// </summary>
    private int? _maxQueueSize;

    /// <summary>
    /// Максимальный размер тела сообщения
    /// </summary>
    private int? _maxPayloadSize;

    /// <summary>
    /// Разрешенный диапазон приоритетов
    /// </summary>
    private (long Min, long Max)? _priorityRange;

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

        _maxQueueSize = maxSize;
        return this;
    }


    public ITaskQueueBuilder WithPriorityRange(long min, long max)
    {
        if (max < min)
        {
            throw new ArgumentOutOfRangeException(nameof(min), min,
                $"Наименьшее значение приоритета не может быть больше максимального. Получен диапазон: {min} - {max}");
        }

        _priorityRange = (min, max);
        return this;
    }

    public ITaskQueueBuilder WithQueueImplementation(PriorityQueueCode implementation)
    {
        if (!Enum.IsDefined(implementation))
        {
            throw new InvalidEnumArgumentException(nameof(implementation), (int)implementation,
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

    public ITaskQueueBuilder WithLastRecordId(RecordId id)
    {
        _lastRecordId = id;
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

        return new TaskQueue(_lastRecordId, _name, queue, policies,
            _queueSubscriberManagerFactory.CreateQueueSubscriberManager());
    }

    private IPriorityQueue<PriorityQueueData> BuildPriorityQueue()
    {
        switch (_queueCode)
        {
            case PriorityQueueCode.Heap4Arity:
                return new HeapPriorityQueue<PriorityQueueData>();
            case PriorityQueueCode.QueueArray:
                if (_priorityRange is var (min, max))
                {
                    return new QueueArrayPriorityQueue<PriorityQueueData>(min, max);
                }

                throw new InvalidOperationException("Диапазон ключей для структуры списка очередей не указан");
        }

        Debug.Assert(Enum.IsDefined(_queueCode), "Enum.IsDefined(_queueCode)",
            "Некорректный код реализации приоритетной очереди");
        throw new ArgumentOutOfRangeException(nameof(_queueCode), _queueCode, "Неизвестный код приоритетной очереди");
    }

    private void FillPriorityQueue(IEnumerable<QueueRecord> records, IPriorityQueue<PriorityQueueData> queue)
    {
        foreach (var (id, key, message) in records)
        {
            queue.Enqueue(key, new PriorityQueueData(id, message));
        }
    }

    private QueuePolicy[] BuildPolicies()
    {
        var result = new List<QueuePolicy>();

        if (_maxQueueSize is { } maxSize)
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