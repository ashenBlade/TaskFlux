using System.Buffers;
using System.Net;
using System.Text;
using Utils.Serialization;

namespace TaskFlux.Network.Packets.Packets;

public class ClusterMetadataResponsePacket : Packet
{
    /// <summary>
    /// Адреса узлов
    /// </summary>
    public IReadOnlyList<EndPoint> EndPoints { get; }

    /// <summary>
    /// Id текущего лидера кластера
    /// </summary>
    public int? LeaderId { get; }

    /// <summary>
    /// Id ответившего узла
    /// </summary>
    public int RespondingId { get; }

    public ClusterMetadataResponsePacket(IReadOnlyList<EndPoint> endPoints, int? leaderId, int respondingId)
    {
        EndPoints = endPoints;
        LeaderId = leaderId;
        RespondingId = respondingId;
    }

    public override PacketType Type => PacketType.ClusterMetadataResponse;


    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        // Думаю, тут пулинг не нужен - этот пакет отправляется очень редко
        var endPoints = new string[EndPoints.Count];
        for (var i = 0; i < EndPoints.Count; i++)
        {
            endPoints[i] = SerializeEndpoint(EndPoints[i]);
        }

        var size = sizeof(PacketType)
                 + sizeof(int) // Длина массива адресов
                 + ( sizeof(int) * endPoints.Length
                   + endPoints.Sum(static e => Encoding.UTF8.GetByteCount(e)) ) // Сами адресы
                 + sizeof(int)                                                  // Id лидера
                 + sizeof(int);                                                 // Id текущего узла
        var array = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var memory = array.AsMemory(0, size);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write(( byte ) PacketType.ClusterMetadataResponse);
            writer.Write(endPoints.Length);
            foreach (var t in endPoints)
            {
                writer.Write(t);
            }

            if (LeaderId is { } leaderId)
            {
                writer.Write(leaderId);
            }
            else
            {
                // ReSharper disable once RedundantCast (явно укажем тип int)
                writer.Write(( int ) -1); // В дополняющем коде все биты будут выставлены в 1
            }

            writer.Write(RespondingId);

            await stream.WriteAsync(memory, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(array);
        }
    }

    private static string SerializeEndpoint(EndPoint endPoint)
    {
        return endPoint switch
               {
                   DnsEndPoint dnsEndPoint =>
                       $"{dnsEndPoint.Host}:{dnsEndPoint.Port}", // Иногда он может вернуть "Unknown/host:port"
                   IPEndPoint ipEndPoint => ipEndPoint.ToString(),
                   _                     => endPoint.ToString() ?? endPoint.Serialize().ToString()
               };
    }

    public new static async Task<ClusterMetadataResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var endpointsCount = await reader.ReadInt32Async(token);

        EndPoint[] endpoints;
        if (endpointsCount == 0)
        {
            endpoints = Array.Empty<EndPoint>();
        }
        else
        {
            endpoints = new EndPoint[endpointsCount];
            for (int i = 0; i < endpointsCount; i++)
            {
                var endpointString = await reader.ReadStringAsync(token);
                endpoints[i] = ParseEndPoint(endpointString);
            }
        }

        int? leaderId = await reader.ReadInt32Async(token);
        if (leaderId == -1)
        {
            leaderId = null;
        }

        var respondingId = await reader.ReadInt32Async(token);
        return new ClusterMetadataResponsePacket(endpoints, leaderId, respondingId);
    }

    private static EndPoint ParseEndPoint(string address)
    {
        if (IPEndPoint.TryParse(address, out var ipEndPoint))
        {
            return ipEndPoint;
        }

        var semicolonIndex = address.IndexOf(':');
        int port;
        string host;
        if (semicolonIndex == -1)
        {
            port = 0;
            host = address;
        }
        else if (int.TryParse(address.AsSpan(semicolonIndex + 1), out port))
        {
            host = address[..semicolonIndex];
        }
        else
        {
            throw new ArgumentException($"Не удалось спарсить адрес порта в адресе {address}");
        }

        if (Uri.CheckHostName(host) != UriHostNameType.Dns)
        {
            throw new ArgumentException(
                $"В переданной строке адреса указана невалидный DNS хост. Полный адрес: {address}. Хост: {host}");
        }

        return new DnsEndPoint(host, port);
    }

    public override ValueTask AcceptAsync(IAsyncPacketVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}