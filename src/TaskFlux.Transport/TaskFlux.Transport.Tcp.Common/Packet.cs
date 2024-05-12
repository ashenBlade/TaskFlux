using TaskFlux.Network.Exceptions;
using TaskFlux.Network.Packets;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network;

public abstract class Packet
{
    protected internal Packet()
    {
    }

    public abstract PacketType Type { get; }
    public abstract ValueTask SerializeAsync(Stream stream, CancellationToken token);

    /// <summary>
    /// Прочитать пакет из указанного потока
    /// </summary>
    /// <param name="stream">Поток из которого нужно десериализовать пакет</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="UnknownPacketException">Получен неизвестный маркер пакета</exception>
    /// <exception cref="UnknownCommandTypeException">Получен неизвестный маркер команды</exception>
    /// <exception cref="UnknownResponseTypeException">Получен неизвестный маркер ответа</exception>
    /// <exception cref="UnknownAuthorizationMethodException">Получен неизвестный маркер типа авторизации</exception>
    /// <exception cref="UnknownQueuePolicyException">Получен неизвестный маркер политики очереди</exception>
    /// <exception cref="EndOfStreamException"><paramref name="stream"/> закрылся</exception>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    /// <returns>Десериализованный пакет</returns>
    public static async ValueTask<Packet> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var marker = await reader.ReadByteAsync(token);
        // Отсортировал в порядке частотности получения пакетов (взял из головы - статистики пока нет)
        switch ((PacketType)marker)
        {
            case PacketType.CommandRequest:
                return await CommandRequestPacket.DeserializeAsync(stream, token);
            case PacketType.AcknowledgeRequest:
                return AcknowledgeRequestPacket.Instance;
            case PacketType.NegativeAcknowledgementRequest:
                return NegativeAcknowledgeRequestPacket.Instance;
            case PacketType.ClusterMetadataRequest:
                return ClusterMetadataRequestPacket.Instance;
            case PacketType.AuthorizationRequest:
                return await AuthorizationRequestPacket.DeserializeAsync(stream, token);
            case PacketType.BootstrapRequest:
                return await BootstrapRequestPacket.DeserializeAsync(stream, token);

            // Пакеты ответов обрабатываются в последнюю очередь - ответы вообще-то мы посылаем
            case PacketType.Ok:
                return OkPacket.Instance;
            case PacketType.ErrorResponse:
                return await ErrorResponsePacket.DeserializeAsync(stream, token);
            case PacketType.NotLeader:
                return await NotLeaderPacket.DeserializeAsync(stream, token);
            case PacketType.AuthorizationResponse:
                return await AuthorizationResponsePacket.DeserializeAsync(stream, token);
            case PacketType.BootstrapResponse:
                return await BootstrapResponsePacket.DeserializeAsync(stream, token);
            case PacketType.ClusterMetadataResponse:
                return await ClusterMetadataResponsePacket.DeserializeAsync(stream, token);
            case PacketType.CommandResponse:
                return await CommandResponsePacket.DeserializeAsync(stream, token);
        }

        throw new UnknownPacketException(marker);
    }
}