using System.Net.Sockets;
using TaskFlux.Network;

namespace TaskFlux.Host.Modules.SocketRequest;

public class TaskFluxClient : ITaskFluxClient
{
    /// <summary>
    /// Поток, который используется для взаимодействия.
    /// На проде - это <see cref="NetworkStream"/>
    /// </summary>
    private readonly Stream _stream;

    public TaskFluxClient(Stream stream)
    {
        _stream = stream;
    }

    public async Task SendAsync(Packet packet, CancellationToken token)
    {
        await packet.SerializeAsync(_stream, token);
    }

    public async Task<Packet> ReceiveAsync(CancellationToken token)
    {
        return await Packet.DeserializeAsync(_stream, token);
    }
}