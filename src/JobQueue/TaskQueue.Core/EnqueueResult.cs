namespace TaskQueue.Core;

public abstract class EnqueueResult
{
    private static readonly SuccessEnqueueResult SuccessResult = new();
    public static SuccessEnqueueResult Success() => SuccessResult;
    public static MaxSizeExceededResult MaxSizeExceeded(int maxSize) => new(maxSize);
    public static PriorityRangeViolationResult PriorityRangeViolation(long min, long max) => new(min, max);
    public static MaxPayloadSizeViolationResult MaxPayloadSizeViolation(uint maxPayloadSize) => new(maxPayloadSize);

    public abstract bool IsSuccess { get; }

    public class MaxSizeExceededResult : EnqueueResult
    {
        public int MaxSize { get; }
        public override bool IsSuccess => false;

        public MaxSizeExceededResult(int maxSize)
        {
            MaxSize = maxSize;
        }
    }

    public class PriorityRangeViolationResult : EnqueueResult
    {
        public long MinValue { get; }
        public long MaxValue { get; }
        public override bool IsSuccess => false;

        public PriorityRangeViolationResult(long minValue, long maxValue)
        {
            MinValue = minValue;
            MaxValue = maxValue;
        }
    }

    public class MaxPayloadSizeViolationResult : EnqueueResult
    {
        public override bool IsSuccess => false;
        public uint MaxPayloadSize { get; }

        public MaxPayloadSizeViolationResult(uint maxPayloadSize)
        {
            MaxPayloadSize = maxPayloadSize;
        }
    }

    public class SuccessEnqueueResult : EnqueueResult
    {
        public override bool IsSuccess => true;
    }
}