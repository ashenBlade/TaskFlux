using System.Collections;
using TaskFlux.Core.Queue;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Core.Restore;

public class QueueInfo
{
    public QueueInfo(QueueName queueName,
                     PriorityQueueCode code,
                     int? maxQueueSize,
                     int? maxPayloadSize,
                     (long, long)? priorityRange)
    {
        QueueName = queueName;
        Code = code;
        MaxQueueSize = maxQueueSize;
        MaxPayloadSize = maxPayloadSize;
        PriorityRange = priorityRange;
        Data = new QueueRecordsValuesCollection(this);
    }

    /// <summary>
    /// Название очереди
    /// </summary>
    public QueueName QueueName { get; }

    /// <summary>
    /// Код реализации очереди
    /// </summary>
    public PriorityQueueCode Code { get; }

    /// <summary>
    /// Максимальный размер очереди, если есть
    /// </summary>
    public int? MaxQueueSize { get; }

    /// <summary>
    /// Максимальный размер нагрузки, если есть
    /// </summary>
    public int? MaxPayloadSize { get; }

    /// <summary>
    /// Допустимый диапазон приоритетов, если есть
    /// </summary>
    public (long, long)? PriorityRange { get; }

    /// <summary>
    /// Записи, которые хранятся в очереди
    /// </summary>
    /// <remarks>
    /// Данные хранятся в неупорядоченном виде
    /// </remarks>
    public IReadOnlyCollection<QueueRecord> Data { get; }

    /// <summary>
    /// Отображение ключа на множество его записей, вместо хранения абсолютно всех записей
    /// </summary>
    private readonly Dictionary<long, HashSet<ByteArrayCounter>> _records = new();

    public void Add(long priority, byte[] payload)
    {
        // Если записи с этим ключом уже существуют
        if (_records.TryGetValue(priority, out var existingSet))
        {
            var counter = new ByteArrayCounter(payload);

            // И если для этого множества тоже есть массивы с таким же содержимым
            if (existingSet.TryGetValue(counter, out var existingArray))
            {
                // То увеличиваем это количество
                existingArray.Increment();
            }
            else
            {
                // Иначе добавляем новую запись
                existingSet.Add(counter);
            }
        }
        // Иначе нужно инициализировать новое множество с переданным ключом
        else
        {
            _records.Add(priority,
                new HashSet<ByteArrayCounter>(new ByteArrayCounterEqualityComparer()) {new(payload)});
        }
    }

    public void Remove(long key, byte[] payload)
    {
        if (_records.TryGetValue(key, out var existingSet))
        {
            var record = new ByteArrayCounter(payload);
            if (existingSet.TryGetValue(record, out var existingCounter))
            {
                // Уменьшаем кол-во элементов для этой записи,
                // но не удаляем, т.к. возможно дальше будут еще записи с таким же содержимым
                existingCounter.Decrement();
            }
        }
    }

    private sealed class ByteArrayCounter
    {
        public ByteArrayCounter(byte[] message)
        {
            Message = message;
            // Изначально у нас только 1 элемент хранится
            Count = 1;
        }

        /// <summary>
        /// Сообщение из записи
        /// </summary>
        public byte[] Message { get; }

        /// <summary>
        /// Количество элементов этой записи.
        /// Используем подсчет, вместо целого массива - данные не изменяемы
        /// </summary>
        public int Count { get; private set; }

        public void Increment()
        {
            Count++;
        }

        public void Decrement()
        {
            if (Count > 0)
            {
                Count--;
            }
        }

        private int? _hashcode;

        public override int GetHashCode()
        {
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return _hashcode ??= ComputeHashCode(Message);
        }

        private static int ComputeHashCode(ReadOnlySpan<byte> data)
        {
            // FNV
            const uint prime = 0x01000193;
            var hash = 0x811c9dc5;
            foreach (var b in data)
            {
                hash *= prime;
                hash ^= b;
            }

            return unchecked( ( int ) hash );
        }
    }

    private sealed class ByteArrayCounterEqualityComparer : IEqualityComparer<ByteArrayCounter>
    {
        public bool Equals(ByteArrayCounter? left, ByteArrayCounter? right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (ReferenceEquals(left, null)) return false;
            if (ReferenceEquals(right, null)) return false;
            return left.GetHashCode() == right.GetHashCode() && left.Message.SequenceEqual(right.Message);
        }

        public int GetHashCode(ByteArrayCounter obj)
        {
            return obj.GetHashCode();
        }
    }

    private class QueueRecordsValuesCollection : IReadOnlyCollection<QueueRecord>
    {
        private readonly QueueInfo _queueInfo;

        public QueueRecordsValuesCollection(QueueInfo queueInfo)
        {
            _queueInfo = queueInfo;
        }

        public IEnumerator<QueueRecord> GetEnumerator()
        {
            foreach (var (priority, counters) in _queueInfo._records)
            {
                foreach (var counter in counters.Where(c => c.Count > 0))
                {
                    for (var i = 0; i < counter.Count; i++)
                    {
                        yield return new QueueRecord(priority, counter.Message);
                    }
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private int? _count;

        private int CalculateCount()
        {
            // Суммируем по всем одинаковым ключам и количеству различных записей для этих ключей
            return _queueInfo._records.Sum(r => r.Value.Sum(s => s.Count));
        }

        public int Count => _count ??= CalculateCount();
    }
}