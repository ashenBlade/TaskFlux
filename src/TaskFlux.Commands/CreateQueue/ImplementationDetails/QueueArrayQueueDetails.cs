using TaskFlux.PriorityQueue;

namespace TaskFlux.Commands.CreateQueue.ImplementationDetails;

public class QueueArrayQueueDetails : QueueImplementationDetails
{
    public int? MaxQueueSize
    {
        init => SetMaxQueueSize(value);
    }

    public int? MaxPayloadSize
    {
        init => SetMaxPayloadSize(value);
    }

    public QueueArrayQueueDetails((long, long) priorityRange) : base(PriorityQueueCode.QueueArray)
    {
        SetPriorityRange(priorityRange);
    }
}