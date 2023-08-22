using System.Collections;
using Consensus.StateMachine.TaskFlux.Serialization;
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

    public void Serialize(Stream destination, IJobQueue queue, CancellationToken token = default)
    {
        // Была идея сделать писателя в поток буферизирующим,
        // но лучше сразу передавать забуферизированный поток (BufferedStream)
        // - меньше головной боли по поводу реализации
        var writer = new StreamBinaryWriter(destination);

        // Заголовок
        var metadata = queue.Metadata;
        writer.Write(metadata.QueueName);
        writer.Write(metadata.MaxSize);
        var count = metadata.Count;
        writer.Write(count);

        if (count == 0)
        {
            destination.Flush();
            return;
        }

        token.ThrowIfCancellationRequested();

        // Сами данные
        foreach (var (priority, payload) in queue.GetAllData())
        {
            writer.Write(priority);
            writer.Write(payload);
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
    /// <returns>Десериализованная очередь</returns>
    public IJobQueue Deserialize(Stream file, CancellationToken token = default)
    {
        var reader = new StreamBinaryReader(file);

        // 1. Название
        // 2. Максимальная длина
        // 3. Текущая длина
        var name = reader.ReadQueueName();
        var maxSize = reader.ReadUInt32();
        var count = reader.ReadUInt32();

        IReadOnlyCollection<(long, byte[])> elements =
            count == 0
                ? Array.Empty<(long, byte[])>()
                : new StreamQueueElementsCollection(file, count);
        return _factory.CreateJobQueue(name, maxSize, elements);
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

        public int Count { get; }
    }
}