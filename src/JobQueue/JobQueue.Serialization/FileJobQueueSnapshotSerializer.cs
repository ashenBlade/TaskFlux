using Consensus.StateMachine.TaskFlux.Serialization;
using JobQueue.Core;
using TaskFlux.Serialization.Helpers;

namespace JobQueue.Serialization;

public class FileJobQueueSnapshotSerializer : IJobQueueSnapshotSerializer
{
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
            return;
        }

        // Сами данные
        foreach (var (priority, payload) in queue.GetAllData())
        {
            writer.Write(priority);
            writer.Write(payload);
        }

        destination.Flush();
    }
}