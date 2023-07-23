using TaskFlux.Network.Requests;

namespace TaskFlux.Network.Client;

public interface ITaskFluxClient
{
    public ValueTask SendAsync(Packet packet, CancellationToken token = default);
    public ValueTask<Packet> ReceiveAsync(CancellationToken token = default);
}