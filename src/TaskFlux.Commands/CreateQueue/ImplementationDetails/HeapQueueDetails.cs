using TaskFlux.PriorityQueue;

namespace TaskFlux.Commands.CreateQueue.ImplementationDetails;

public class HeapQueueDetails : QueueImplementationDetails
{
    public (long, long)? PriorityRange
    {
        init => SetPriorityRange(value);
    }

    public int? MaxQueueSize
    {
        init => SetMaxQueueSize(value);
    }

    public int? MaxPayloadSize
    {
        init => SetMaxPayloadSize(value);
    }

    public HeapQueueDetails() : base(PriorityQueueCode.Heap4Arity)
    {
    }
}