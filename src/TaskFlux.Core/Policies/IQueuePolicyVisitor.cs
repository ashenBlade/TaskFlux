namespace TaskFlux.Core.Policies;

public interface IQueuePolicyVisitor<out TReturn>
{
    public TReturn Visit(PriorityRangeQueuePolicy policy);
    public TReturn Visit(MaxQueueSizeQueuePolicy policy);
    public TReturn Visit(MaxPayloadSizeQueuePolicy policy);
}