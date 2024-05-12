using System.Net;
using System.Net.Sockets;
using TaskFlux.Network;
using TaskFlux.Network.Authorization;
using TaskFlux.Network.Packets;
using TaskFlux.Transport.Tcp.Client.Exceptions;

namespace TaskFlux.Transport.Tcp.Client;

public class TaskFluxClientFactory
{
    private readonly EndPoint[] _endPoints;

    /// <summary>
    /// Id текущего лидера кластера.
    /// Равен индексу в массиве <see cref="_endPoints"/>.
    /// Если лидер не известен, то равен <c>null</c>
    /// </summary>
    internal int? LeaderId { get; set; }

    public TaskFluxClientFactory(EndPoint[] endPoints)
    {
        ArgumentNullException.ThrowIfNull(endPoints);
        if (endPoints.Length == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(endPoints), endPoints, "Массив адресов серверов пуст");
        }

        _endPoints = endPoints;
    }


    public async Task<ITaskFluxClient> ConnectAsync(CancellationToken token)
    {
        var client = await ConnectCoreAsync(token);
        return new ReconnectingTaskFluxClientDecorator(client, this);
    }

    internal async Task<TaskFluxClient> ConnectCoreAsync(CancellationToken token)
    {
        if (LeaderId is { } savedLeaderId)
        {
            var stream = await EstablishConnectionToHostAsync(_endPoints[savedLeaderId], token);
            if (stream is not null)
            {
                return new TaskFluxClient(stream);
            }
        }

        foreach (var endPoint in _endPoints)
        {
            var stream = await EstablishConnectionToHostAsync(endPoint, token);
            if (stream is not null)
            {
                return new TaskFluxClient(stream);
            }
        }

        throw new ClusterUnavailableException();
    }

    private static async Task<NetworkStream?> EstablishConnectionToHostAsync(EndPoint endPoint, CancellationToken token)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            // 1. Устанавливаем TCP соединение
            await socket.ConnectAsync(endPoint, token);
            var stream = new NetworkStream(socket);
            // 2. Авторизуемся
            await AuthorizeAsync(stream, token);

            // 3. Бутстрапимся
            await BootstrapAsync(stream, token);

            // 4. Готово
            return stream;
        }
        catch (Exception) // Так плохо, но легче
        {
            socket.Dispose();
        }

        return null;
    }

    private static async Task BootstrapAsync(NetworkStream stream, CancellationToken token)
    {
        var version = Constants.ClientVersion;
        var request = new BootstrapRequestPacket(version.Major, version.Minor, version.Build);
        await request.SerializeAsync(stream, token);

        var response = await Packet.DeserializeAsync(stream, token);
        Helpers.CheckNotErrorResponse(response);

        if (response.Type != PacketType.BootstrapResponse)
        {
            throw new UnexpectedPacketException(PacketType.BootstrapResponse, response.Type);
        }

        var bootstrapResponse = (BootstrapResponsePacket)response;
        if (!bootstrapResponse.Success)
        {
            throw new BootstrapException(bootstrapResponse.Reason);
        }
    }

    private static async Task AuthorizeAsync(NetworkStream stream, CancellationToken token)
    {
        var authRequestPacket = new AuthorizationRequestPacket(new NoneAuthorizationMethod());
        await authRequestPacket.SerializeAsync(stream, token);
        var responsePacket = await Packet.DeserializeAsync(stream, token);
        Helpers.CheckNotErrorResponse(responsePacket);
        if (responsePacket.Type != PacketType.AuthorizationResponse)
        {
            throw new UnexpectedPacketException(PacketType.AuthorizationResponse, responsePacket.Type);
        }

        var authResponsePacket = (AuthorizationResponsePacket)responsePacket;
        if (!authResponsePacket.Success)
        {
            throw new AuthorizationException(authResponsePacket.ErrorReason);
        }
    }
}