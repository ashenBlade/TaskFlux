using System.Collections;
using Consensus.Application.TaskFlux.Serialization;
using TaskFlux.Abstractions;
using TaskFlux.Core;
using Utils.Serialization;

namespace TaskFlux.Serialization;

public class FileTaskQueueSnapshotSerializer : ITaskQueueSnapshotSerializer
{
    private readonly ITaskQueueFactory _factory;

    public FileTaskQueueSnapshotSerializer(ITaskQueueFactory factory)
    {
        _factory = factory;
    }

    public byte[] Serialize(ITaskQueue queue)
    {
        var memory = new MemoryStream();
        var writer = new StreamBinaryWriter(memory);

        Serialize(queue, ref writer);

        return memory.ToArray();
    }


    /// <summary>
    /// Сериализовать все переданные очереди в поток (файл) снапшота
    /// </summary>
    /// <param name="destination">Поток для записи данных</param>
    /// <param name="queues">Очереди, которые нужно сериализовать</param>
    /// <param name="token">Токен отмены</param>
    public void Serialize(Stream destination, IEnumerable<ITaskQueue> queues, CancellationToken token = default)
    {
        // Была идея сделать писателя в поток буферизирующим,
        // но лучше сразу передавать забуферизированный поток (BufferedStream)
        // - меньше головной боли по поводу реализации
        var writer = new StreamBinaryWriter(destination);

        foreach (var queue in queues)
        {
            Serialize(queue, ref writer);
            token.ThrowIfCancellationRequested();
        }

        destination.Flush();
    }

    /// <summary>
    /// Описание структуры формата сериализованной очереди
    /// </summary>
    /// <remarks>
    /// Нужно только для разработки - нигде лучше не использовать
    /// </remarks>
    // ReSharper disable once UnusedMember.Local
    private const string QueueFileFormat = "\n"
                                         + "Название (QueueName)\n"
                                         + "Максимальный размер очереди (Int32, -1 = нет лимита)\n"
                                         + "Диапазон ключей (Nullable<Pair<long, long>>)\n"
                                         + "Максимальный размер сообщения (Int32, -1 = нет лимита)\n"
                                         + "Содержимое очереди (размер + данные)";

    /// <summary>
    /// Сериализовать одну очередь, используя переданный <paramref name="writer"/>
    /// </summary>
    /// <param name="queue">Очередь, которую нужно сериализовать</param>
    /// <param name="writer">Объект для записи данных</param>
    private static void Serialize(IReadOnlyTaskQueue queue, ref StreamBinaryWriter writer)
    {
        // 1. Заголовок
        var metadata = queue.Metadata;

        // 1.1 Название очереди
        writer.Write(metadata.QueueName);

        // 1.2 Установленный максимальный размер
        if (metadata.MaxSize is { } maxSize)
        {
            writer.Write(maxSize);
        }
        else
        {
            writer.Write(-1);
        }

        // 1.3 Диапазон ключей
        if (metadata.PriorityRange is var (min, max))
        {
            writer.Write(true);
            writer.Write(min);
            writer.Write(max);
        }
        else
        {
            writer.Write(false);
        }

        // 1.4 Максимальный размер сообщения
        if (metadata.MaxPayloadSize is { } maxPayloadSize)
        {
            writer.Write(true);
            writer.Write(maxPayloadSize);
        }
        else
        {
            writer.Write(false);
        }

        // 2. Сами данные очереди
        var count = metadata.Count;
        writer.Write(count);

        if (count == 0)
        {
            // Заканчиваем, если очередь пуста
            return;
        }

        foreach (var (priority, payload) in queue.ReadAllData())
        {
            writer.Write(priority);
            writer.WriteBuffer(payload);
        }
    }

    /// <summary>
    /// Десериализовать одну очередь из потока
    /// </summary>
    /// <param name="file">
    /// Поток файла, в котором на текущей позиции находится сериализованное представление очереди
    /// </param>
    /// <param name="token">
    /// Токен отмены
    /// </param>
    /// <returns>Десериализованные очереди</returns>
    /// <remarks>
    /// Данный метод только десериализует очереди.
    /// Проверка бизнес-правил остается на вызывающем
    /// </remarks>
    public IEnumerable<ITaskQueue> Deserialize(Stream file, CancellationToken token = default)
    {
        var reader = new StreamBinaryReader(file);

        var result = new List<ITaskQueue>();

        while (!reader.IsEnd)
        {
            var name = reader.ReadQueueName();

            // Максимальный размер очереди
            int? maxSize = reader.ReadInt32();
            if (maxSize == -1)
            {
                maxSize = null;
            }

            // Диапазон значений приоритетов/ключей
            (long Min, long Max)? priorityRange = null;
            if (reader.ReadBool())
            {
                var min = reader.ReadInt64();
                var max = reader.ReadInt64();
                priorityRange = ( min, max );
            }

            // Максимальный размер нагрузки
            int? maxPayloadSize = null;
            if (reader.ReadBool())
            {
                maxPayloadSize = reader.ReadInt32();
            }

            // Сами элементы
            var count = reader.ReadInt32();

            IReadOnlyCollection<(long, byte[])> elements =
                count == 0
                    ? Array.Empty<(long, byte[])>()
                    : new StreamQueueElementsCollection(file, count);

            var queue = _factory.CreateTaskQueue(name, maxSize, priorityRange, maxPayloadSize, elements);
            result.Add(queue);
        }

        return result;
    }

    private class StreamQueueElementsCollection : IReadOnlyCollection<(long, byte[])>
    {
        private readonly Stream _stream;
        public int Count { get; }

        public StreamQueueElementsCollection(Stream stream, int count)
        {
            _stream = stream;
            Count = count;
        }

        public IEnumerator<(long, byte[])> GetEnumerator()
        {
            var reader = new StreamBinaryReader(_stream);
            for (var i = 0; i < Count; i++)
            {
                var priority = reader.ReadInt64();
                var payload = reader.ReadBuffer();
                yield return ( priority, payload );
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}