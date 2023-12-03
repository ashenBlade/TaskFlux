namespace TaskFlux.Network.Packets.Commands;

public interface INetworkCommandVisitor<out T>
{
    T Visit(CountNetworkCommand command);
    T Visit(CreateQueueNetworkCommand command);
    T Visit(DeleteQueueNetworkCommand command);
    T Visit(ListQueuesNetworkCommand command);
    T Visit(EnqueueNetworkCommand command);
    T Visit(DequeueNetworkCommand command);
}