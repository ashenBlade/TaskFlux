using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using TaskFlux.Network.Requests.Authorization;
using TaskFlux.Network.Requests.Packets;
using TaskFlux.Network.Requests.Serialization.Exceptions;
using TaskFlux.Serialization.Helpers;

namespace TaskFlux.Network.Requests.Serialization;

public class PoolingNetworkPacketSerializer: IAsyncPacketVisitor
{
    private const int ByteFalse = 0;
    private Stream Stream { get; }
    private ArrayPool<byte> Pool { get; }

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
    public async Task<Packet> DeserializeAsync(CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var marker = await GetPacketTypeAsync(token);
        return marker switch
               {
                   PacketType.CommandRequest        => await DeserializeDataRequest(token),
                   PacketType.CommandResponse       => await DeserializeDataResponse(token),
                   PacketType.ErrorResponse         => await DeserializeErrorResponse(token),
                   PacketType.NotLeader             => await DeserializeNotLeaderResponse(token),
                   PacketType.AuthorizationRequest  => await DeserializeAuthorizationRequest(token),
                   PacketType.AuthorizationResponse => await DeserializeAuthorizationResponse(token),
                   PacketType.BootstrapRequest      => await DeserializeBootstrapRequest(token),
                   PacketType.BootstrapResponse     => await DeserializeBootstrapResponse(token),
               };
    }

    private async Task<BootstrapResponsePacket> DeserializeBootstrapResponse(CancellationToken token)
    {
        var success = await ReadBool(token);
        if (success)
        {
            return BootstrapResponsePacket.Ok;
        }

        var message = await ReadString(PacketType.BootstrapResponse, token);
        return BootstrapResponsePacket.Error(message);
    }

    private async Task<BootstrapRequestPacket> DeserializeBootstrapRequest(CancellationToken token)
    {
        var major = await ReadInt32(token);
        var minor = await ReadInt32(token);
        var patch = await ReadInt32(token);
        return new BootstrapRequestPacket(major, minor, patch);
    }

    public Task SerializeAsync(Packet packet, CancellationToken token = default)
    {
        return packet.AcceptAsync(this, token).AsTask();
    }

    private async Task<AuthorizationResponsePacket> DeserializeAuthorizationResponse(CancellationToken token)
    {
        var success = await ReadBool(token);
        if (success)
        {
            return AuthorizationResponsePacket.Ok;
        }

        var reason = await ReadString(PacketType.AuthorizationResponse, token);
        return AuthorizationResponsePacket.Error(reason);
    }

    private async Task<string> ReadString(PacketType packetType, CancellationToken token)
    {
        var length = await ReadInt32(token);
        var buffer = Pool.Rent(length);
        try
        {
            var index = 0;
            var left = length;
            while (0 < left)
            {
                var read = await Stream.ReadAsync(buffer.AsMemory(index, left), token);
                if (read == 0)
                {
                    ThrowEndOfStream();
                }

                left -= read;
                index += read;
            }

            return Encoding.UTF8.GetString(buffer.AsSpan(0, length));
        }
        catch (DecoderFallbackException fallbackException)
        {
            throw new PacketDeserializationException(packetType, $"Ошибка десериализации строки", fallbackException);
        }
        finally
        {
            Pool.Return(buffer);
        }
    }

    private async Task<bool> ReadBool(CancellationToken token)
    {
        var b = await ReadByte(token);
        return b != ByteFalse;
    }

    private async Task<Packet> DeserializeAuthorizationRequest(CancellationToken token)
    {
        var authMethod = await DeserializeAuthorizationMethod(token);
        return new AuthorizationRequestPacket(authMethod);
    }
    private async Task<AuthorizationMethod> DeserializeAuthorizationMethod(CancellationToken token)
    {
        var authType = await ReadByte(token);
        try
        {
            return ( AuthorizationMethodType ) authType switch
                   {
                       AuthorizationMethodType.None => await DeserializeNoneAuthorization(token),
                   };
        }
        catch (SwitchExpressionException)
        {
            throw new PacketDeserializationException(PacketType.AuthorizationRequest,
                $"Неизвестный маркер типа авторизации: {authType}");
        }
    }
    
    private Task<NoneAuthorizationMethod> DeserializeNoneAuthorization(CancellationToken token)
    {
        return Task.FromResult(NoneAuthorizationMethod.Instance);
    }

    private async Task<byte> ReadByte(CancellationToken token)
    {
        var buffer = Pool.Rent(sizeof(byte));
        try
        {
            var read = await Stream.ReadAsync(buffer.AsMemory(0, sizeof(AuthorizationMethodType)), token);
            if (read == 0)
            {
                ThrowEndOfStream();
            }

            return buffer[0];
        }
        finally
        {
            Pool.Return(buffer);
        }
    }

    private async Task<NotLeaderPacket> DeserializeNotLeaderResponse(CancellationToken token)
    {
        var id = await ReadInt32(token);
        return new NotLeaderPacket(id);
    }

    private async Task<ErrorResponsePacket> DeserializeErrorResponse(CancellationToken token)
    {
        var length = await ReadInt32(token);
        var buffer = Pool.Rent(length);
        try
        {
            var left = length;
            var index = 0;
            while (0 < left)
            {
                var read = await Stream.ReadAsync(buffer.AsMemory(index, left), token);
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

    private async Task<CommandResponsePacket> DeserializeDataResponse(CancellationToken token)
    {
        var buffer = await ReadBuffer(token);
        return new CommandResponsePacket(buffer);
    }

    private async Task<CommandRequestPacket> DeserializeDataRequest(CancellationToken token)
    {
        var buffer = await ReadBuffer(token);
        return new CommandRequestPacket(buffer);
    }

    private async Task<byte[]> ReadBuffer(CancellationToken token)
    {
        var length = await ReadInt32(token);
        if (length == 0)
        {
            return Array.Empty<byte>();
        }

        var buffer = Pool.Rent(length);
        try
        {
            var left = length;
            var index = 0;
            while (0 < left)
            {
                var read = await Stream.ReadAsync(buffer.AsMemory(index, left), token);
                if (read == 0)
                {
                    ThrowEndOfStream();
                }
                index += read;
                left -= read;
            }

            var result = new byte[length];
            buffer.AsSpan(0, length).CopyTo(result);
            return result;
        }
        finally
        {
            Pool.Return(buffer);
        }
    }

    private async Task<int> ReadInt32(CancellationToken token)
    {
        var buffer = Pool.Rent(sizeof(int));
        try
        {
            var index = 0;
            var left = sizeof(int);
            while (0 < left)
            {
                var read = await Stream.ReadAsync(buffer.AsMemory(index, left), token);
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

    private async Task<PacketType> GetPacketTypeAsync(CancellationToken token = default)
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

    public async ValueTask VisitAsync(AuthorizationRequestPacket packet, CancellationToken token = default)
    {
        const int baseSize = sizeof(PacketType);
        using var visitor = new PayloadSerializerAuthorizationMethodVisitor(Pool);
        packet.AuthorizationMethod.Accept(visitor);
        var buffer = visitor.Buffer;
        var resultSize = baseSize + buffer.Length;
        var resultBuffer = Pool.Rent(resultSize);
        try
        {
            var memory = resultBuffer.AsMemory(0, resultSize);
            memory.Span[0] = ( byte ) PacketType.AuthorizationRequest;
            buffer.CopyTo(memory[baseSize..]);
            await Stream.WriteAsync(memory, token);
        }
        finally
        {
            Pool.Return(resultBuffer);
        }
    }

    private class PayloadSerializerAuthorizationMethodVisitor: IAuthorizationMethodVisitor, IDisposable
    {
        private readonly ArrayPool<byte> _pool;
        private byte[]? _buffer;
        private Memory<byte>? _memory;

        public Memory<byte> Buffer => _memory
                                   ?? throw new InvalidOperationException(
                                          "Обнаружена попытка обратиться к неинициализированному буферу сериализатора пакетов авторизации");

        public PayloadSerializerAuthorizationMethodVisitor(ArrayPool<byte> pool)
        {
            _pool = pool;
        }
        
        public void Visit(NoneAuthorizationMethod noneAuthorizationMethod)
        {
            const int size = sizeof(AuthorizationMethodType);
            var buffer = _pool.Rent(size);
            try
            {
                var memory = new Memory<byte>(buffer, 0, size);
                memory.Span[0] = ( byte ) AuthorizationMethodType.None;
                _buffer = buffer;
                _memory = memory;
            }
            catch (Exception)
            {
                _pool.Return(buffer);
                throw;
            }
        }

        public void Dispose()
        {
            if (_buffer is not null)
            {
                _pool.Return(_buffer);
            }
        }
    }

    public async ValueTask VisitAsync(AuthorizationResponsePacket packet, CancellationToken token = default)
    {
        if (packet.TryGetError(out var error))
        {
            var errorSize = sizeof(PacketType)
                          + sizeof(bool)
                          + sizeof(int)
                          + Encoding.UTF8.GetByteCount(error);
            var buffer = Pool.Rent(errorSize);
            try
            {
                var memory = buffer.AsMemory(0, errorSize);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write((byte)PacketType.AuthorizationResponse);
                writer.Write(false);
                writer.Write(error);
                await Stream.WriteAsync(memory, token);
            }
            finally
            {
                Pool.Return(buffer);
            }
        }
        else
        {
            const int successSize = sizeof(PacketType)
                                  + sizeof(bool);
            var buffer = Pool.Rent(successSize);
            try
            {
                var memory = buffer.AsMemory(0, successSize);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write((byte)PacketType.AuthorizationResponse);
                writer.Write(true);
                await Stream.WriteAsync(memory, token);
            }
            finally
            {
                Pool.Return(buffer);
            }
        }
    }

    public async ValueTask VisitAsync(BootstrapResponsePacket packet, CancellationToken token = default)
    {
        if (packet.TryGetError(out var message))
        {
            var length = sizeof(PacketType)
                       + sizeof(byte)
                       + sizeof(int)
                       + Encoding.UTF8.GetByteCount(message);
            var buffer = Pool.Rent(length);
            try
            {
                var memory = buffer.AsMemory(0, length);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write((byte)PacketType.BootstrapResponse);
                writer.Write(false);
                writer.Write(message);
                await Stream.WriteAsync(memory, token);
            }
            finally
            {
                Pool.Return(buffer);   
            }
        }
        else
        {
            const int length = sizeof(PacketType)
                             + sizeof(byte);
            var buffer = Pool.Rent(length);
            try
            {
                var memory = buffer.AsMemory(0, length);
                var writer = new MemoryBinaryWriter(memory);
                writer.Write((byte)PacketType.BootstrapResponse);
                writer.Write(true);
                await Stream.WriteAsync(memory, token);
            }
            finally
            {
                Pool.Return(buffer);
            }
        }
    }

    public async ValueTask VisitAsync(BootstrapRequestPacket packet, CancellationToken token = default)
    {
        const int length = sizeof(PacketType)
                         + sizeof(int)
                         + sizeof(int)
                         + sizeof(int);
        var buffer = Pool.Rent(length);
        try
        {
            var memory = buffer.AsMemory(0, length);
            var writer = new MemoryBinaryWriter(memory);
            writer.Write((byte)PacketType.BootstrapRequest);
            writer.Write(packet.Major);
            writer.Write(packet.Minor);
            writer.Write(packet.Patch);
            await Stream.WriteAsync(memory, token);
        }
        finally
        {
            Pool.Return(buffer);
        }
    }
}