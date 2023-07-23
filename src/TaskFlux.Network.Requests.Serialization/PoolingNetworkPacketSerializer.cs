using System.Buffers;
using System.Text;
using TaskFlux.Network.Requests.Packets;
using System.Buffers.Binary;
using TaskFlux.Serialization.Helpers;

namespace TaskFlux.Network.Requests.Serialization;

public class PoolingNetworkPacketSerializer
{
    public ArrayPool<byte> Pool { get; }

    public PoolingNetworkPacketSerializer(ArrayPool<byte> pool)
    {
        Pool = pool;
    }
    
    public PooledBuffer Serialize(Packet packet)
    {
        var size = EstimateSize(packet);
        var buffer = Pool.Rent(size);
        try
        {
            var serializerVisitor = new SerializerPacketVisitor(buffer);
            packet.Accept(serializerVisitor);
            return new PooledBuffer(buffer, size, Pool);
        }
        catch (Exception)
        {
            Pool.Return(buffer);
            throw;
        }
    }

    private class SerializerPacketVisitor : IPacketVisitor
    {
        private MemoryBinaryWriter _writer;
        
        public SerializerPacketVisitor(byte[] buffer)
        {
            _writer = new MemoryBinaryWriter(buffer);
        }

        private void WriteMarker(PacketType packetType)
        {
            _writer.Write((byte)packetType);
        }
        
        public void Visit(DataRequestPacket packet)
        {
            WriteMarker(PacketType.DataRequest);
            _writer.WriteBuffer(packet.Payload);
        }

        public void Visit(DataResponsePacket packet)
        {
            WriteMarker(PacketType.DataResponse);
            _writer.WriteBuffer(packet.Payload);
        }

        public void Visit(ErrorResponsePacket packet)
        {
            WriteMarker(PacketType.ErrorResponse);
            _writer.Write(packet.Message);
        }

        public void Visit(NotLeaderPacket packet)
        {
            WriteMarker(PacketType.NotLeader);
            _writer.Write(packet.LeaderId);
        }
    }

    private static int EstimateSize(Packet packet)
    {
        var estimator = new SizeEstimatorVisitor();
        packet.Accept(estimator);
        return estimator.EstimatedSize;
    }

    private class SizeEstimatorVisitor : IPacketVisitor
    {
        public const int DefaultSize = sizeof(PacketType); // Только маркер
        
        public int EstimatedSize { get; set; }
        public void Visit(DataRequestPacket packet)
        {
            EstimatedSize = DefaultSize            // Маркер
                          + sizeof(int)            // Длина
                          + packet.Payload.Length; // Тело
        }

        public void Visit(DataResponsePacket packet)
        {
            EstimatedSize = DefaultSize            // Маркер
                          + sizeof(int)            // Длина
                          + packet.Payload.Length; // Тело
        }

        public void Visit(ErrorResponsePacket packet)
        {
            EstimatedSize = DefaultSize                                 // Маркер
                          + sizeof(int)                                 // Длина
                          + Encoding.UTF8.GetByteCount(packet.Message); // Само сообщение
        }

        public void Visit(NotLeaderPacket packet)
        {
            EstimatedSize = DefaultSize  // Маркер
                          + sizeof(int); // LeaderId
        }
    }

    public async ValueTask<Packet> DeserializeAsync(Stream stream, CancellationToken token = default)
    {
        var marker = await GetPacketTypeAsync(stream, token);
        return marker switch
               {
                   PacketType.DataRequest   => await DeserializeDataRequest(stream, token),
                   PacketType.DataResponse  => await DeserializeDataResponse(stream, token),
                   PacketType.ErrorResponse => await DeserializeErrorResponse(stream, token),
                   PacketType.NotLeader     => await DeserializeNotLeaderResponse(stream, token),
               };
    }

    private async ValueTask<NotLeaderPacket> DeserializeNotLeaderResponse(Stream stream, CancellationToken token)
    {
        var id = await ReadInt32(stream, token);
        return new NotLeaderPacket(id);
    }

    private async ValueTask<ErrorResponsePacket> DeserializeErrorResponse(Stream stream, CancellationToken token)
    {
        var length = await ReadInt32(stream, token);
        var buffer = Pool.Rent(length);
        try
        {
            var left = length;
            var index = 0;
            while (0 < left)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(index, left), token);
                left -= read;
                index += read;
            }

            var message = Encoding.UTF8.GetString(buffer.AsSpan(0, length));
            return new ErrorResponsePacket(message);
        }
        finally
        {
            Pool.Return(buffer);
        }
    }

    private async ValueTask<DataResponsePacket> DeserializeDataResponse(Stream stream, CancellationToken token)
    {
        var buffer = await ReadBuffer(stream, token);
        return new DataResponsePacket(buffer);
    }

    private async ValueTask<DataRequestPacket> DeserializeDataRequest(Stream stream, CancellationToken token)
    {
        var buffer = await ReadBuffer(stream, token);
        return new DataRequestPacket(buffer);
    }

    private async ValueTask<byte[]> ReadBuffer(Stream stream, CancellationToken token)
    {
        var length = await ReadInt32(stream, token);
        if (length == 0)
        {
            return Array.Empty<byte>();
        }
        
        var buffer = new byte[length];
        var left = length;
        var index = 0;
        while (0 < left)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(index, left), token);
            index += read;
            left -= read;
        }

        return buffer;
    }

    private async ValueTask<int> ReadInt32(Stream stream, CancellationToken token)
    {
        var buffer = Pool.Rent(sizeof(int));
        try
        {
            var index = 0;
            var left = sizeof(int);
            while (0 < left)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(index, left), token);
                index += read;
                left -= read;
            }

            return BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, sizeof(int)));
        }
        finally
        {
            Pool.Return(buffer);
        }
    }

    public async ValueTask<PacketType> GetPacketTypeAsync(Stream stream, CancellationToken token = default)
    {
        var markerBuffer = Pool.Rent(1);
        try
        {
            var read = await stream.ReadAsync(markerBuffer.AsMemory(0, 1), token);
            if (read == 0)
            {
                throw new EndOfStreamException("Не удалось прочитать маркер пакета. Достигнут конец потока");
            }

            return ( PacketType ) markerBuffer[0];
        }
        finally
        {
            Pool.Return(markerBuffer);
        }
    }
}