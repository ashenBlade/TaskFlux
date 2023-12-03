using TaskFlux.Network.Packets.Exceptions;
using TaskFlux.Network.Packets.Packets;
using Utils.Serialization;

namespace TaskFlux.Network.Packets;

public abstract class Packet
{
    protected internal Packet()
    {
    }

    public abstract PacketType Type { get; }
    public abstract ValueTask SerializeAsync(Stream stream, CancellationToken token);
    public abstract ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default);

    /// <summary>
    /// Прочитать пакет из указанного потока
    /// </summary>
    /// <param name="stream">Поток из которого нужно десериализовать пакет</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Десерилазованный пакет</returns>
    public static async ValueTask<Packet> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var marker = await reader.ReadByteAsync(token);
        switch (( PacketType ) marker)
        {
            case PacketType.CommandRequest:
                return await CommandRequestPacket.DeserializeAsync(stream, token);
            case PacketType.CommandResponse:
                return await CommandResponsePacket.DeserializeAsync(stream, token);
            case PacketType.AcknowledgeRequest:
                return AcknowledgeRequestPacket.Instance;
            case PacketType.ErrorResponse:
                return await ErrorResponsePacket.DeserializeAsync(stream, token);
            case PacketType.NotLeader:
                return await NotLeaderPacket.DeserializeAsync(stream, token);
            case PacketType.AuthorizationRequest:
                return await AuthorizationRequestPacket.DeserializeAsync(stream, token);
            case PacketType.AuthorizationResponse:
                return await AuthorizationResponsePacket.DeserializeAsync(stream, token);
            case PacketType.BootstrapRequest:
                return await BootstrapRequestPacket.DeserializeAsync(stream, token);
            case PacketType.BootstrapResponse:
                return await BootstrapResponsePacket.DeserializeAsync(stream, token);
            case PacketType.ClusterMetadataRequest:
                return ClusterMetadataRequestPacket.Instance;
            case PacketType.ClusterMetadataResponse:
                return await ClusterMetadataResponsePacket.DeserializeAsync(stream, token);
        }

        throw new UnknownPacketException(marker);
    }
}