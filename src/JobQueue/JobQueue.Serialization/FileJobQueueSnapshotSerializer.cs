using System.Collections;
using Consensus.Application.TaskFlux.Serialization;
using JobQueue.Core;
using TaskFlux.Serialization.Helpers;

namespace JobQueue.Serialization;

public class FileJobQueueSnapshotSerializer : IJobQueueSnapshotSerializer
{
    private readonly IJobQueueFactory _factory;

    public FileJobQueueSnapshotSerializer(IJobQueueFactory factory)
    {
        _factory = factory;
    }

    public byte[] Serialize(IJobQueue queue)
    {
        var memory = new MemoryStream();
        var writer = new StreamBinaryWriter(memory);
        // Заголовок
        var metadata = queue.Metadata;
        writer.Write(metadata.QueueName);
        writer.Write(metadata.MaxSize);
        var count = metadata.Count;
        writer.Write(count);

        if (count == 0)
        {
            return memory.ToArray();
        }

        foreach (var (priority, payload) in queue.GetAllData())
        {
            writer.Write(priority);
            writer.WriteBuffer(payload);
        }

        return memory.ToArray();
    }

    /// <summary>
    /// Сериализовать все переданные очереди в поток (файл) снапшота
    /// </summary>
    /// <param name="destination">Поток для записи данных</param>
    /// <param name="queues">Очереди, которые нужно сериализовать</param>
    /// <param name="token">Токен отмены</param>
    public void Serialize(Stream destination, IEnumerable<IJobQueue> queues, CancellationToken token = default)
    {
        // Была идея сделать писателя в поток буферизирующим,
        // но лучше сразу передавать забуферизированный поток (BufferedStream)
        // - меньше головной боли по поводу реализации
        var writer = new StreamBinaryWriter(destination);
        foreach (var queue in queues)
        {
            // Заголовок
            var metadata = queue.Metadata;
            writer.Write(metadata.QueueName);
            writer.Write(metadata.MaxSize);
            var count = metadata.Count;
            writer.Write(count);

            if (count == 0)
            {
                continue;
            }

            foreach (var (priority, payload) in queue.GetAllData())
            {
                writer.Write(priority);
                writer.WriteBuffer(payload);
            }

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
    public IEnumerable<IJobQueue> Deserialize(Stream file, CancellationToken token = default)
    {
        var reader = new StreamBinaryReader(file);

        var result = new List<IJobQueue>();

        while (!reader.IsEnd)
        {
            var name = reader.ReadQueueName();
            var maxSize = reader.ReadUInt32();
            var count = reader.ReadUInt32();

            IReadOnlyCollection<(long, byte[])> elements =
                count == 0
                    ? Array.Empty<(long, byte[])>()
                    : new StreamQueueElementsCollection(file, count);

            result.Add(_factory.CreateJobQueue(name, maxSize, elements));
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