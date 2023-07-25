using System.Buffers;
using System.Text;
using TaskFlux.Network.Requests.Packets;
using System.Buffers.Binary;
using TaskFlux.Network.Requests.Serialization.Exceptions;
using TaskFlux.Serialization.Helpers;

namespace TaskFlux.Network.Requests.Serialization;

public class PoolingNetworkPacketSerializer: IAsyncPacketVisitor
{
    public Stream Stream { get; }
    public ArrayPool<byte> Pool { get; }

    public PoolingNetworkPacketSerializer(ArrayPool<byte> pool, Stream stream)
    {
        Stream = stream;
        Pool = pool;
    }

    /// <summary>
    /// Прочитать следующий пакет из потока
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Десериализованный пакет</returns>
    /// <exception cref="OperationCanceledException"> <paramref name="token"/> был отменен</exception>
    /// <exception cref="EndOfStreamException"> - был достигнут конец потока</exception>
    /// <exception cref="PacketDeserializationException"> ошибка при десериализации конкретного пакета</exception>
    public async ValueTask<Packet> DeserializeAsync(CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var marker = await GetPacketTypeAsync(token);
        return marker switch
               {
                   PacketType.CommandRequest   => await DeserializeDataRequest(Stream, token),
                   PacketType.CommandResponse  => await DeserializeDataResponse(Stream, token),
                   PacketType.ErrorResponse => await DeserializeErrorResponse(Stream, token),
                   PacketType.NotLeader     => await DeserializeNotLeaderResponse(Stream, token),
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
                if (read == 0)
                {
                    ThrowEndOfStream();
                }
                left -= read;
                index += read;
            }
            
            string message;
            try
            {
                message = Encoding.UTF8.GetString(buffer.AsSpan(0, length));
            }
            catch (DecoderFallbackException fallback)
            {
                throw new PacketDeserializationException(PacketType.ErrorResponse, $"Ошибка десериализации строки сообщения ошибки из пакета {nameof(PacketType.ErrorResponse)}", fallback);
            }

            return new ErrorResponsePacket(message);
        }
        finally
        {
            Pool.Return(buffer);
        }
    }
    
    private static void ThrowEndOfStream() => throw new EndOfStreamException("Был достигнут конец потока");

    private async ValueTask<CommandResponsePacket> DeserializeDataResponse(Stream stream, CancellationToken token)
    {
        var buffer = await ReadBuffer(stream, token);
        return new CommandResponsePacket(buffer);
    }

    private async ValueTask<CommandRequestPacket> DeserializeDataRequest(Stream stream, CancellationToken token)
    {
        var buffer = await ReadBuffer(stream, token);
        return new CommandRequestPacket(buffer);
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
            if (read == 0)
            {
                ThrowEndOfStream();
            }
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
                if (read == 0)
                {
                    ThrowEndOfStream();
                }
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

    private async ValueTask<PacketType> GetPacketTypeAsync(CancellationToken token = default)
    {
        var markerBuffer = Pool.Rent(1);
        try
        {
            var read = await Stream.ReadAsync(markerBuffer.AsMemory(0, 1), token);
            if (read == 0)
            {
                ThrowEndOfStream();            
            }

            return ( PacketType ) markerBuffer[0];
        }
        finally
        {
            Pool.Return(markerBuffer);
        }
    }

    public async ValueTask VisitAsync(CommandRequestPacket packet, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var estimatedSize = sizeof(PacketType)
                          + sizeof(int)
                          + packet.Payload.Length;
        var array = Pool.Rent(estimatedSize);
        try
        {
            var buffer = array.AsMemory(0, estimatedSize);
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)PacketType.CommandRequest);
            writer.WriteBuffer(packet.Payload);
            await Stream.WriteAsync(buffer, token);
        }
        finally
        {
            Pool.Return(array);
        }
    }

    public async ValueTask VisitAsync(CommandResponsePacket packet, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var estimatedSize = sizeof(PacketType)
                          + sizeof(int)
                          + packet.Payload.Length;
        var array = Pool.Rent(estimatedSize);
        try
        {
            var buffer = array.AsMemory(0, estimatedSize);
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)PacketType.CommandResponse);
            writer.WriteBuffer(packet.Payload);
            await Stream.WriteAsync(buffer, token);
        }
        finally
        {
            Pool.Return(array);
        }
    }

    public async ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var estimatedSize = sizeof(PacketType)
                          + sizeof(int)
                          + Encoding.UTF8.GetByteCount(packet.Message);
        var array = Pool.Rent(estimatedSize);
        try
        {
            var buffer = array.AsMemory(0, estimatedSize);
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)PacketType.ErrorResponse);
            writer.Write(packet.Message);
            await Stream.WriteAsync(buffer, token);
        }
        finally
        {
            Pool.Return(array);
        }
    }

    public async ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        const int estimatedSize = sizeof(PacketType)
                                + sizeof(int);
        var array = Pool.Rent(estimatedSize);
        try
        {
            var buffer = array.AsMemory(0, estimatedSize);
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)PacketType.NotLeader);
            writer.Write(packet.LeaderId);
            await Stream.WriteAsync(buffer, token);
        }
        finally
        {
            Pool.Return(array);
        }
    }
}