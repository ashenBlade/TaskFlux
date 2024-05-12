namespace TaskFlux.Network.Responses.Policies;

public interface IPolicyVisitor<out T>
{
    public T Visit(GenericNetworkQueuePolicy queuePolicy);
    public T Visit(MaxMessageSizeNetworkQueuePolicy queuePolicy);
    public T Visit(MaxQueueSizeNetworkQueuePolicy queuePolicy);
    public T Visit(PriorityRangeNetworkQueuePolicy queuePolicy);
}