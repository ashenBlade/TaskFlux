using System.Collections;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.PriorityQueue;
using TaskFlux.Utils.CheckSum;

namespace TaskFlux.Application.Persistence;

internal class QueueInfo
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
    }

    public QueueName QueueName { get; }
    public PriorityQueueCode Code { get; }
    public int? MaxQueueSize { get; }
    public int? MaxPayloadSize { get; }
    public (long, long)? PriorityRange { get; }

    /// <summary>
    /// Отображение ключа на множество его записей.
    /// Для оптимизации, используем 
    /// </summary>
    private readonly Dictionary<long, HashSet<ByteArrayCounter>> _records = new();

    public ITaskQueue Build()
    {
        return new TaskQueueBuilder(QueueName, Code)
              .WithPriorityRange(PriorityRange)
              .WithMaxQueueSize(MaxQueueSize)
              .WithMaxPayloadSize(MaxPayloadSize)
              .WithData(new QueueRecordsValuesCollection(this))
              .Build();
    }

    public void Add(long key, byte[] payload)
    {
        // Если записи с этим ключом уже существуют
        if (_records.TryGetValue(key, out var existingSet))
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
            _records.Add(key, new HashSet<ByteArrayCounter>(new ByteArrayCounterEqualityComparer()) {new(payload)});
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
            return _hashcode ??= ( int ) Crc32CheckSum.Compute(Message);
        }
    }

    private sealed class ByteArrayCounterEqualityComparer : IEqualityComparer<ByteArrayCounter>
    {
        public bool Equals(ByteArrayCounter? left, ByteArrayCounter? right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (ReferenceEquals(left, null)) return false;
            if (ReferenceEquals(right, null)) return false;
            return left.Message.Length == right.Message.Length && left.Message.SequenceEqual(right.Message);
        }

        public int GetHashCode(ByteArrayCounter obj)
        {
            return obj.GetHashCode();
        }
    }

    public (QueueName Name, PriorityQueueCode Code, int? MaxQueueSize, int? MaxPayloadSize, (long, long)? PriorityRange,
        IReadOnlyCollection<(long Key, byte[] Payload)> Data) ToTuple()
    {
        return ( QueueName, Code, MaxQueueSize, MaxPayloadSize, PriorityRange, new QueueRecordsValuesCollection(this) );
    }

    private class QueueRecordsValuesCollection : IReadOnlyCollection<(long Key, byte[] Data)>
    {
        private readonly QueueInfo _queueInfo;

        public QueueRecordsValuesCollection(QueueInfo queueInfo)
        {
            _queueInfo = queueInfo;
        }

        public IEnumerator<(long Key, byte[] Data)> GetEnumerator()
        {
            foreach (var (key, counters) in _queueInfo._records)
            {
                foreach (var counter in counters.Where(c => c.Count > 0))
                {
                    for (var i = 0; i < counter.Count; i++)
                    {
                        yield return ( key, counter.Message );
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