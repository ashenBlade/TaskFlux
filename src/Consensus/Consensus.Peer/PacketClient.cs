using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Consensus.Network;

namespace Consensus.Peer;

public class PacketClient: IDisposable
{
    private readonly IPacketSerializer _serializer;
    public Socket Socket { get; }
    private NetworkStream Stream { get; }

    public PacketClient(Socket socket, IPacketSerializer serializer)
    {
        _serializer = serializer;
        Socket = socket;
        Stream = new(socket, false);
    }


    public ValueTask<bool> SendAsync(IPacket requestPacket, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        return SendCore(requestPacket, token);
    }

    private async ValueTask<bool> SendCore(IPacket packet, CancellationToken token)
    {
        if (!Socket.Connected)
        {
            return false;
        }

        
        var requiredSize = packet.EstimatePacketSize();
        var buffer = ArrayPool<byte>.Shared.Rent(requiredSize);
        try
        {
            _serializer.Serialize(packet, new BinaryWriter(new MemoryStream(buffer)));
            await Stream.WriteAsync(buffer.AsMemory(), token);
            return true;
        }
        catch (SocketException se)
            when (IsNetworkError(se.SocketErrorCode))
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public ValueTask<IPacket?> ReceiveAsync(CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        if (!Socket.Connected)
        {
            return new ValueTask<IPacket?>((IPacket?) null);
        }

        try
        {
            return ReceiveCoreAsync(token);
        }
        catch (SocketException se)
            when (IsNetworkError(se.SocketErrorCode))
        {
            return new ValueTask<IPacket?>((IPacket?) null);
        }
        catch (IOException)
        {
            return new ValueTask<IPacket?>((IPacket?) null);
        }
    }

    private async ValueTask<IPacket?> ReceiveCoreAsync(CancellationToken token)
    {
        // Требуемый размер буфера для десериализации пакета
        const int workHeaderSize = sizeof(byte) + sizeof(int);
        byte[]? resultBuffer = null;
        try
        {
            // Вначале, получаем рабочий заголовок: маркер + размер
            int totalPacketSize;
            var workHeaderBuffer = ArrayPool<byte>.Shared.Rent(workHeaderSize);
            try
            {
                var received = await Stream.ReadAsync(workHeaderBuffer.AsMemory(0, workHeaderSize), token);
                if (received != workHeaderSize || !Socket.Connected)
                {
                    await Socket.DisconnectAsync(true, token);
                    return null;
                }

                totalPacketSize = ReadTotalPacketSize(workHeaderBuffer);
                resultBuffer = ArrayPool<byte>.Shared.Rent(totalPacketSize);
                workHeaderBuffer.CopyTo(resultBuffer, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(workHeaderBuffer);
            }
            
            // После читаем рабочую нагрузку
            var left = totalPacketSize - workHeaderSize;
            var startIndex = workHeaderSize;
                
            while (0 < left)
            {
                var received = await Stream.ReadAsync(resultBuffer.AsMemory(startIndex, left), token);
                if (received == 0)
                {
                    await Socket.DisconnectAsync(true, token);
                    return null;
                }

                startIndex += received;
                left -= received;
            }
            
            return resultBuffer is {Length: > 0}
                       ? _serializer.Deserialize(new BinaryReader(new MemoryStream(resultBuffer)))
                       : null;

        }
        finally
        {
            if (resultBuffer is not null)
            {
                ArrayPool<byte>.Shared.Return(resultBuffer);
            }
        }
        
        static int ReadTotalPacketSize(byte[] initialBuffer)
        {
            var sizePart = initialBuffer.AsSpan(1, 4);
            var size = BinaryPrimitives.ReadInt32LittleEndian(sizePart);
            if (size < 1)
            {
                throw new InvalidDataException(
                    $"Ошибка десериализации размера буфера. Размер должен быть положительным. Десериализовано: {size}");
            }
            return size;
        }
    }

    private static bool IsNetworkError(SocketError error)
    {
        return error is 
                   SocketError.Shutdown or 
                   SocketError.NotConnected or 
                   SocketError.Disconnecting or 
                   
                   SocketError.ConnectionReset or 
                   SocketError.ConnectionAborted or 
                   SocketError.ConnectionRefused or 
                   
                   SocketError.NetworkDown or 
                   SocketError.NetworkReset or 
                   SocketError.NetworkUnreachable or 
                   
                   SocketError.HostDown or 
                   SocketError.HostUnreachable or 
                   SocketError.HostNotFound;
    }

    public async ValueTask<bool> ConnectAsync(EndPoint endPoint, TimeSpan timeout, CancellationToken token = default)
    {
        if (Socket.Connected)
        {
            return true;
        }
        
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            cts.CancelAfter(timeout);
            await Socket.ConnectAsync(endPoint, cts.Token);
        }
        catch (SocketException)
        {
            return false;
        }
        // Незадокументировано
        catch (IOException io) when (io.GetBaseException() is SocketException)
        {
            return false;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        
        return true;
    }

    public async ValueTask DisconnectAsync(CancellationToken token = default)
    {
        if (Socket.Connected)
        {
            await Socket.DisconnectAsync(true, token);
        }
    }

    public void Dispose()
    {
        Stream.Flush();
    }
}