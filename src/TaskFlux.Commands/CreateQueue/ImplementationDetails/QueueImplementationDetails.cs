using TaskFlux.PriorityQueue;

namespace TaskFlux.Commands.CreateQueue.ImplementationDetails;

public abstract class QueueImplementationDetails
{
    private (long, long)? _priorityRange;

    protected void SetPriorityRange((long, long)? range)
    {
        if (range is var (min, max) && max < min)
        {
            throw new InvalidPriorityRangeException(min, max);
        }

        _priorityRange = range;
    }

    private int? _maxQueueSize;

    protected void SetMaxQueueSize(int? maxQueueSize)
    {
        if (maxQueueSize is { } mqs and < 0)
        {
            throw new InvalidMaxQueueSizeException(mqs);
        }

        _maxQueueSize = maxQueueSize;
    }

    private int? _maxPayloadSize;

    protected void SetMaxPayloadSize(int? maxPayloadSize)
    {
        if (maxPayloadSize is { } mps and < 0)
        {
            throw new InvalidMaxPayloadSizeException(mps);
        }

        _maxPayloadSize = maxPayloadSize;
    }

    public PriorityQueueCode Code { get; }

    protected internal QueueImplementationDetails(PriorityQueueCode code)
    {
        Code = code;
    }

    public bool TryGetPriorityRange(out long min, out long max)
    {
        ( min, max ) = _priorityRange.GetValueOrDefault();
        return _priorityRange.HasValue;
    }

    public bool TryGetMaxPayloadSize(out int maxPayloadSize)
    {
        maxPayloadSize = _maxPayloadSize.GetValueOrDefault();
        return _maxPayloadSize.HasValue;
    }

    public bool TryGetMaxQueueSize(out int maxQueueSize)
    {
        maxQueueSize = _maxQueueSize.GetValueOrDefault();
        return _maxQueueSize.HasValue;
    }
}