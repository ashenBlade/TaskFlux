using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Raft.Core;
using Raft.Peer.Exceptions;
using Serilog;

namespace Raft.Peer;

public class RaftTcpSocket: ISocket
{
    private readonly string _host;
    private readonly int _port;
    private readonly PeerId _nodeId;
    private readonly TimeSpan _requestTimeout;
    private readonly int _receiveBufferSize;
    private readonly ILogger _logger;
    private readonly Socket _socket;

    public RaftTcpSocket(string host, int port, PeerId nodeId, TimeSpan requestTimeout, int receiveBufferSize, ILogger logger)
    {
        _host = host;
        _port = port;
        _nodeId = nodeId;
        _requestTimeout = requestTimeout;
        _receiveBufferSize = receiveBufferSize;
        _logger = logger;
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    }

    public async Task SendAsync(ReadOnlyMemory<byte> payload, CancellationToken token = default)
    {
        await CheckConnectionAsync(token);

        var left = payload.Length;
        try
        {
            int sent;
            using (var cts = CreateTimeoutLinkedTokenSource())
            {
                sent = await _socket.SendAsync(payload, SocketFlags.None, cts.Token);
            }
        
            left -= sent;
            while (0 < left)
            {
                using (var cts = CreateTimeoutLinkedTokenSource())
                {
                    sent = await _socket.SendAsync(payload[sent..], SocketFlags.None, cts.Token);
                }
                left -= sent;
                sent += sent;
            }
        
            CancellationTokenSource CreateTimeoutLinkedTokenSource()
            {
                var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
                cts.CancelAfter(_requestTimeout);
                return cts;
            }
        }
        catch (SocketException socket) when (IsNetworkError(socket.SocketErrorCode))
        {
            throw new NetworkException(socket);
        }
    }

    private async ValueTask CheckConnectionAsync(CancellationToken token)
    {
        if (!_socket.Connected)
        {
            await EstablishConnectionAsync(token);
        }
    }

    private async Task EstablishConnectionAsync(CancellationToken token)
    {
        _logger.Debug("Делаю запрос подключения на хост {Host} и порт {Port}", _host, _port);
        using (var cts = CreateTimeoutLinkedTokenSource())
        {
            await Task.Run(() =>
            {
                var result = _socket.BeginConnect(_host, _port, null, null);
                var  success = result.AsyncWaitHandle.WaitOne(_requestTimeout, true);
                if (success)
                {
                    _socket.EndConnect(result);
                }
                else
                {
                    throw new NetworkException(new SocketException(10060)); // Connection timed out
                }
            }, cts.Token);
        }
        
        try
        {
            _logger.Debug("Соединение установлено. Делаю запрос авторизации");
            var connectionPacket = CreateAuthorizationPacket();
            var totalSent = 0;
            while (totalSent != connectionPacket.Length)
            {
                using var cts = CreateTimeoutLinkedTokenSource();
                var sent = await _socket.SendAsync(connectionPacket.AsMemory(totalSent), SocketFlags.None, cts.Token);
                totalSent += sent;
            }
            
            _logger.Debug("Запрос авторизации выполнен. Ожидаю ответа");
            var buffer = new byte[2];
            using (var cts = CreateTimeoutLinkedTokenSource())
            {
                _ = await _socket.ReceiveAsync(buffer, SocketFlags.None, cts.Token);
            }

            if (CheckSuccess(buffer))
            {
                _logger.Debug("Авторизация прошла успешно");
            }
            else
            {
                _logger.Debug("Авторизоваться не удалось");
                throw new AuthenticationException("Не удалось авторизоваться на узле");
            }
        }
        catch (SocketException)
        {
            await _socket.DisconnectAsync(true, token);
            throw;
        }
        

        bool CheckSuccess(byte[] data)
        {
            return data[0] is ( byte ) RequestType.Connect && data[1] != 0;
        }
        
        byte[] CreateAuthorizationPacket()
        {
            var memory = new MemoryStream(5);
            var writer = new BinaryWriter(memory, Encoding.Default, true);
            writer.Write(( byte ) RequestType.Connect);
            writer.Write(_nodeId.Value);
            return memory.ToArray();
        }

        CancellationTokenSource CreateTimeoutLinkedTokenSource()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            cts.CancelAfter(_requestTimeout);
            return cts;
        }
    }

    // ReSharper disable once MethodHasAsyncOverloadWithCancellation
    public async ValueTask ReadAsync(Stream stream, CancellationToken token = default)
    {
        if (!stream.CanWrite)
        {
            throw new NotSupportedException("Переданный поток не поддерживает запись");
        }
        
       
        await CheckConnectionAsync(token);
        var buffer = new byte[_receiveBufferSize];
        while (token.IsCancellationRequested is false)
        {
            int read;
            using (var cts = CreateTimeoutLinkedTokenSource())
            {
                read = await _socket.ReceiveAsync(buffer, SocketFlags.None, cts.Token);
            }
            stream.Write(buffer, 0, read);
            if (read < buffer.Length)
            {
                break;
            }
        }
        
        
        CancellationTokenSource CreateTimeoutLinkedTokenSource()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            cts.CancelAfter(_requestTimeout);
            return cts;
        }
    }

    private static bool IsNetworkError(SocketError error)
    {
        return error is
                   SocketError.HostDown or
                   SocketError.HostUnreachable or
                   SocketError.HostNotFound or
                   SocketError.NetworkDown or
                   SocketError.NetworkUnreachable or
                   SocketError.NetworkReset or 
                   SocketError.ConnectionRefused or 
                   SocketError.ConnectionReset;
    }

    public void Dispose()
    {
        _socket.Dispose();
    }
}