using System.Collections;
using TaskFlux.Core;
using TaskFlux.Core.Queue;
using TaskFlux.PriorityQueue;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Persistence;

/// <summary>
/// Объект для сериализации и десериализации очередей в файл снапшота.
/// Сериализуются именно очереди - без вспомогательных данных 
/// </summary>
public static class QueuesSnapshotSerializer
{
    public static IReadOnlyCollection<ReadOnlyMemory<byte>> Serialize(IReadOnlyCollection<IReadOnlyTaskQueue> queues)
    {
        if (queues.Count == 0)
        {
            return CreateEmptySnapshot();
        }

        return new SerializedQueuesSnapshotCollection(queues.Select(q =>
        {
            var metadata = q.Metadata;
            return ( q.Name, q.Code, MaxSize: metadata.MaxQueueSize, metadata.MaxPayloadSize, metadata.PriorityRange,
                     q.ReadAllData() );
        }), queues.Count);
    }

    private static IReadOnlyCollection<ReadOnlyMemory<byte>> CreateEmptySnapshot()
    {
        var result = new byte[sizeof(int)];
        var writer = new MemoryBinaryWriter(result);
        writer.Write(0);
        return new ReadOnlyMemory<byte>[] {result};
    }

    public static IEnumerable<ReadOnlyMemory<byte>> Serialize(QueueCollection collection)
    {
        if (collection.IsEmpty)
        {
            return CreateEmptySnapshot();
        }

        return new SerializedQueuesSnapshotCollection(collection.GetQueuesRaw(), collection.Count);
    }

    private class SerializedQueuesSnapshotCollection : IReadOnlyCollection<ReadOnlyMemory<byte>>
    {
        private readonly IEnumerable<(QueueName Name, PriorityQueueCode Code, int? MaxMessageSize, int? MaxPayloadSize,
            (long, long)? PriorityRange, IReadOnlyCollection<(long, byte[])> Data)> _queues;

        /// <summary>
        /// Каждая очередь будет представлять собой отдельный чанк.
        /// Количество чанков равно количеству очередей,
        /// т.к. размер массива (кол-во чанков) будет передаваться уже в первом чанке вместе с первой очередью
        /// </summary>
        public int Count { get; }

        public SerializedQueuesSnapshotCollection(
            IEnumerable<(QueueName, PriorityQueueCode, int? MaxMessageSize, int? MaxPayloadSize, (long, long)?
                PriorityRange, IReadOnlyCollection<(long, byte[])> Data)> queues,
            int count)
        {
            Count = count;
            _queues = queues;
        }

        public IEnumerator<ReadOnlyMemory<byte>> GetEnumerator()
        {
            var memory = new MemoryStream();
            var writer = new StreamBinaryWriter(memory);

            // Сразу записываем кол-во очередей - отправится с первым же чанком
            writer.Write(Count);

            foreach (var (name, code, maxMessageSize, maxPayloadSize, priorityRange, data) in _queues)
            {
                // Сериализуем очередь
                Serialize(name, code, maxMessageSize, maxPayloadSize, priorityRange, data, ref writer);

                // Возвращаем представление очереди
                yield return memory.ToArray();

                // Очищаем перед следующим заходом
                memory.Position = 0;
                memory.SetLength(0);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    /// <summary>
    /// Описание структуры формата сериализованной очереди
    /// </summary>
    /// <remarks>
    /// Нужно только для разработки - нигде лучше не использовать
    /// </remarks>
    // ReSharper disable once UnusedMember.Local
    private const string QueueFileFormat = ""
                                         + "Название (QueueName)\n"
                                         + "Максимальный размер очереди (Int32, -1 = нет лимита)\n"
                                         + "Диапазон ключей (Nullable<Pair<long, long>>)\n"
                                         + "Максимальный размер сообщения (Int32, -1 = нет лимита)\n"
                                         + "Содержимое очереди (размер + данные)";

    private static void Serialize(QueueName name,
                                  PriorityQueueCode code,
                                  int? maxQueueSize,
                                  int? maxMessageSize,
                                  (long, long)? priorityRange,
                                  IReadOnlyCollection<(long, byte[])> data,
                                  ref StreamBinaryWriter writer)
    {
        // Название | Реализация | Максимальный размер очереди | Макс. размер сообщения | Диапазон ключей | Данные очереди

        // 1. Метаданные
        // 1.1 Название
        writer.Write(name);

        // 1.2 Тип реализации
        writer.Write(( int ) code);

        // 1.3 Максимальный размер очереди
        if (maxQueueSize is { } mqs)
        {
            writer.Write(mqs);
        }
        else
        {
            writer.Write(-1);
        }

        // 1.4 Максимальный размер сообщения
        if (maxMessageSize is { } mms)
        {
            writer.Write(mms);
        }
        else
        {
            writer.Write(-1);
        }

        // 1.5 Диапазон ключей
        if (priorityRange is var (min, max))
        {
            writer.Write(true);
            writer.Write(min);
            writer.Write(max);
        }
        else
        {
            writer.Write(false);
        }

        // 2. Сами данные очереди
        var count = data.Count;

        // 2.1 Количество элементов
        writer.Write(count);

        if (count == 0)
        {
            // Заканчиваем, если очередь пуста
            return;
        }

        // 2.2 Сами элементы
        foreach (var (priority, payload) in data)
        {
            writer.Write(priority);
            writer.WriteBuffer(payload);
        }
    }

    /// <summary>
    /// Сериализовать одну очередь, используя переданный <paramref name="writer"/>
    /// </summary>
    /// <param name="queue">Очередь, которую нужно сериализовать</param>
    /// <param name="writer">Объект для записи данных</param>
    private static void Serialize(IReadOnlyTaskQueue queue, ref StreamBinaryWriter writer)
    {
        // Название | Реализация | Максимальный размер очереди | Макс. размер сообщения | Диапазон ключей | Данные очереди

        // 1. Метаданные
        var metadata = queue.Metadata;
        Serialize(metadata.QueueName, metadata.Code, metadata.MaxQueueSize, metadata.MaxPayloadSize,
            metadata.PriorityRange,
            queue.ReadAllData(), ref writer);
    }

    /// <summary>
    /// Десериализовать очереди из снапшота
    /// </summary>
    /// <param name="file">
    /// Снапшот, из которого нужно десериализовать очереди
    /// </param>
    /// <returns>Десериализованные очереди</returns>
    /// <remarks>
    /// Данный метод только десериализует очереди.
    /// Проверка бизнес-правил остается на вызывающем
    /// </remarks>
    public static QueueCollection Deserialize(Stream file)
    {
        var collection = new QueueCollection();

        var reader = new StreamBinaryReader(file);
        var left = reader.ReadInt32();
        for (var i = 0; i < left; i++)
        {
            // Название
            var name = reader.ReadQueueName();

            // Реализация
            var implementation = ( PriorityQueueCode ) reader.ReadInt32();

            // Максимальный размер очереди
            int? maxQueueSize = reader.ReadInt32();
            if (maxQueueSize == -1)
            {
                maxQueueSize = null;
            }

            // Максимальный размер сообщения
            int? maxPayloadSize = reader.ReadInt32();
            if (maxPayloadSize == -1)
            {
                maxPayloadSize = null;
            }

            // Диапазон значений приоритетов/ключей
            (long Min, long Max)? priorityRange = null;
            if (reader.ReadBool())
            {
                var min = reader.ReadInt64();
                var max = reader.ReadInt64();
                priorityRange = ( min, max );
            }


            // Сами элементы
            var count = reader.ReadInt32();
            if (count == 0)
            {
                collection.AddExistingQueue(name, implementation, maxQueueSize, maxPayloadSize, priorityRange,
                    Array.Empty<QueueRecord>());
                continue;
            }

            var records = new List<QueueRecord>(count);

            for (int j = 0; j < count; j++)
            {
                var key = reader.ReadInt64();
                var payload = reader.ReadBuffer();
                records.Add(new QueueRecord(key, payload));
            }

            collection.AddExistingQueue(name, implementation, maxQueueSize, maxPayloadSize, priorityRange, records);
        }

        return collection;
    }
}