using TaskFlux.Core.Policies;
using TaskFlux.Core.Restore;
using TaskFlux.Core.Subscription;
using TaskFlux.Domain;
using TaskFlux.PriorityQueue;
using TaskFlux.PriorityQueue.Heap;

namespace TaskFlux.Core.Queue;

/// <summary>
/// Вспомогательный класс, содержащий значения по умолчанию
/// </summary>
public static class Defaults
{
    public static QueueName QueueName => QueueName.Default;
    public static PriorityQueueCode PriorityQueueCode => PriorityQueueCode.Heap4Arity;
    public static RecordId LastRecordId => RecordId.Start;
    public static int? MaxQueueSize => null;
    public static int? MaxPayloadSize => null;
    public static (long, long)? PriorityRange => null;

    internal static TaskQueue CreateDefaultTaskQueue(IQueueSubscriberManager queueSubscriberManager)
    {
        return new TaskQueue(LastRecordId, QueueName, new HeapPriorityQueue<PriorityQueueData>(),
            Array.Empty<QueuePolicy>(), queueSubscriberManager);
    }

    public static QueueInfo CreateDefaultQueueInfo()
    {
        return new QueueInfo(QueueName, PriorityQueueCode, LastRecordId, MaxQueueSize, MaxPayloadSize, PriorityRange);
    }
}