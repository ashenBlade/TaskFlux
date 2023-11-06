using System.Collections;
using System.Diagnostics;

namespace TaskFlux.PriorityQueue.QueueArray;

/// <summary>
/// Реализация приоритетной очереди,
/// которая использует массив списков,
/// где индексами являются ключи записей
/// </summary>
public class QueueArrayPriorityQueue : IPriorityQueue
{
    /// <summary>
    /// Начальное значение ключа (включительно)
    /// </summary>
    public long Min { get; }

    /// <summary>
    /// Максимальное значение ключа (включительно)
    /// </summary>
    public long Max { get; }

    /// <summary>
    /// Массив из очередей для каждого возможного значения ключа.
    /// Индексы должны корректироваться в соответствии 
    /// </summary>
    private Queue<byte[]>?[]? _queues;

    private Queue<byte[]>?[] GetQueues() => _queues ??= CreateEmptyQueueArray();

    private Queue<byte[]>[] CreateEmptyQueueArray()
    {
        // Берем не только элементы между ними, но и границы включительно
        return new Queue<byte[]>[Max - Min + 1];
    }

    public int Count => _queues?.Sum(x => x?.Count ?? 0) ?? 0;

    public QueueArrayPriorityQueue(long min, long max)
    {
        if (max < min)
        {
            throw new ArgumentException(
                $"Минимальное значение ключа не может быть больше максимального. Максимальное: {max}. Минимальное: {min}");
        }

        Min = min;
        Max = max;
    }

    /// <summary>
    /// Ининциализировать массив уже готовыми элементами.
    /// Этот конструктор нужен для тестов
    /// </summary>
    /// <param name="min">Минимальный ключ</param>
    /// <param name="max">Максимальный ключ</param>
    /// <param name="values">Значения для инициализации</param>
    /// <remarks>Порядок в заполняемых значениях не учитывается. Чтобы порядок сохранился, вручную заполняй</remarks>
    internal QueueArrayPriorityQueue(long min, long max, IEnumerable<(long Key, byte[] Data)> values) : this(min, max)
    {
        var queues = CreateEmptyQueueArray();

        foreach (var (k, e) in values
                              .ToLookup(t => t.Key,
                                   t => t.Data)
                              .ToDictionary(g => KeyToIndex(g.Key)))
        {
            queues[k] = new Queue<byte[]>(e);
        }

        _queues = queues;
    }

    /// <summary>
    /// Превратить переданный ключ в индекс в массиве очередей.
    /// Если переданный ключ выходит за границы, то возникнет соответствующее исключение.
    /// </summary>
    /// <param name="key">Ключ, переданный клиентом</param>
    /// <exception cref="InvalidKeyRangeException"><paramref name="key"/> выходит за возможные границы диапазона</exception>
    private long KeyToIndex(long key)
    {
        if (key < Min || Max < key)
        {
            throw new InvalidKeyRangeException(Max, Min, key, nameof(key));
        }

        var index = key - Min;
        Debug.Assert(index >= 0, "index >= 0", "Индекс в массиве не может быть отрицательным");
        return index;
    }

    /// <summary>
    /// Превратить индекс массива очередей в соответствующий им ключ
    /// </summary>
    /// <param name="index">Индекс в массиве очередей</param>
    /// <returns>Расчитанный приоритет</returns>
    private long IndexToKey(long index)
    {
        var key = index + Min;
        Debug.Assert(Min <= key, "Min <= key", "Рассчитанный ключ не может быть меньше минимального значения ключа");
        Debug.Assert(key <= Max, "key <= Max", "Рассчитанный ключ не может быть меньше максимального значения ключа");
        return key;
    }

    public void Enqueue(long key, byte[] value)
    {
        var index = KeyToIndex(key);
        var queue = GetQueues()[index] ??= new Queue<byte[]>(1);
        queue.Enqueue(value);
    }

    public bool TryDequeue(out long key, out byte[] value)
    {
        var queues = GetQueues();
        for (long i = 0; i < queues.Length; i++)
        {
            var queue = queues[i];
            if (queue?.TryDequeue(out var data) ?? false)
            {
                key = IndexToKey(i);
                value = data;
                return true;
            }
        }

        key = default;
        value = default!;
        return false;
    }

    public IReadOnlyCollection<(long Priority, byte[] Payload)> ReadAllData()
    {
        return new NullableQueueArrayCollection(this);
    }

    private class NullableQueueArrayCollection : IReadOnlyCollection<(long, byte[])>
    {
        private readonly QueueArrayPriorityQueue _parent;

        public NullableQueueArrayCollection(QueueArrayPriorityQueue parent)
        {
            _parent = parent;
        }

        public IEnumerator<(long, byte[])> GetEnumerator()
        {
            var queues = _parent._queues;
            if (queues is null)
            {
                yield break;
            }

            for (int i = 0; i < queues.Length; i++)
            {
                if (queues[i] is {Count: > 0} queue)
                {
                    var key = _parent.IndexToKey(i);
                    foreach (var data in queue)
                    {
                        yield return ( key, data );
                    }
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => _parent.Count;
    }

    internal List<(long, byte[])> ToListUnordered()
    {
        if (_queues is null)
        {
            return new List<(long, byte[])>();
        }

        var result = new List<(long, byte[])>();

        for (var i = 0; i < _queues.Length; i++)
        {
            if (_queues[i] is {Count: > 0} queue)
            {
                var key = IndexToKey(i);
                result.AddRange(queue.Select(d => ( key, d )));
            }
        }

        return result;
    }

    /// <summary>
    /// Метод для получения всех элементов из очереди вместе с их удалением.
    /// То же самое, что и самому вызывать TryDequeue и сохранять результаты в список
    /// </summary>
    /// <returns>Список из прочитанных элементов очереди</returns>
    internal List<(long, byte[])> DequeueAll()
    {
        if (_queues is null)
        {
            return new List<(long, byte[])>();
        }

        var result = new List<(long, byte[])>();

        for (var i = 0; i < _queues.Length; i++)
        {
            if (_queues[i] is {Count: > 0} queue)
            {
                var key = IndexToKey(i);
                while (queue.TryDequeue(out var element))
                {
                    result.Add(( key, element ));
                }
            }
        }

        return result;
    }
}