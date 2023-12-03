namespace TaskFlux.Network.Packets.Responses;

public interface INetworkResponseVisitor<out T>
{
    public T Visit(CountNetworkResponse response);
    public T Visit(DequeueNetworkResponse response);
    public T Visit(ErrorNetworkResponse response);
    public T Visit(ListQueuesNetworkResponse response);
    public T Visit(OkNetworkResponse response);
    public T Visit(PolicyViolationNetworkResponse response);
}