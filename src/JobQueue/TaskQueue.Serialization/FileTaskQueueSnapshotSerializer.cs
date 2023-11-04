using System.Collections;
using Consensus.Application.TaskFlux.Serialization;
using TaskFlux.Serialization.Helpers;
using TaskQueue.Core;

namespace TaskQueue.Serialization;

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

    private static void Serialize(ITaskQueue queue, ref StreamBinaryWriter writer)
    {
        // Заголовок
        var metadata = queue.Metadata;

        // Название очереди
        writer.Write(metadata.QueueName);

        // Установленный максимальный размер
        if (metadata.MaxSize is { } maxSize)
        {
            writer.Write(maxSize);
        }
        else
        {
            writer.Write(-1);
        }

        // Диапазон ключей
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

        // Максимальный размер нагрузки
        if (metadata.MaxPayloadSize is { } maxPayloadSize)
        {
            writer.Write(true);
            writer.Write(maxPayloadSize);
        }
        else
        {
            writer.Write(false);
        }

        var count = metadata.Count;
        writer.Write(count);

        if (count == 0)
        {
            return;
        }

        foreach (var (priority, payload) in queue.ReadAllData())
        {
            writer.Write(priority);
            writer.WriteBuffer(payload);
        }
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
    /// 
    public IEnumerable<ITaskQueue> Deserialize(Stream file, CancellationToken token = default)
    {
        var reader = new StreamBinaryReader(file);

        var result = new List<ITaskQueue>();

        while (!reader.IsEnd)
        {
            var name = reader.ReadQueueName();

            // Максимальный размер очереди
            int? maxSize = reader.ReadInt32();
            if (maxSize != -1)
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
            uint? maxPayloadSize = null;
            if (reader.ReadBool())
            {
                maxPayloadSize = reader.ReadUInt32();
            }

            // Сами элементы
            var count = reader.ReadUInt32();

            IReadOnlyCollection<(long, byte[])> elements =
                count == 0
                    ? Array.Empty<(long, byte[])>()
                    : new StreamQueueElementsCollection(file, count);

            result.Add(_factory.CreateTaskQueue(name, maxSize, priorityRange, maxPayloadSize, elements));
        }

        return result;
    }

    private class StreamQueueElementsCollection : IReadOnlyCollection<(long, byte[])>
    {
        private readonly Stream _stream;
        private readonly uint _count;

        public StreamQueueElementsCollection(Stream stream, uint count)
        {
            _stream = stream;
            _count = count;
        }

        public IEnumerator<(long, byte[])> GetEnumerator()
        {
            var reader = new StreamBinaryReader(_stream);
            for (int i = 0; i < _count; i++)
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

        public int Count => ( int ) _count;
    }
}