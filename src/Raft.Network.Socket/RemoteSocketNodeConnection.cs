using System.Net;
using System.Net.Sockets;
using Serilog;

namespace Raft.Network.Socket;

public class RemoteSocketNodeConnection: SocketNodeConnection, IRemoteNodeConnection
{
    private readonly EndPoint? _endPoint;
    private readonly ILogger _logger;

    public RemoteSocketNodeConnection(EndPoint endPoint, ILogger logger)
        : base(new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
    {
        _endPoint = endPoint;
        _logger = logger;
    }

    public RemoteSocketNodeConnection(System.Net.Sockets.Socket existingSocket, ILogger logger) 
        : base(existingSocket)
    {
        _logger = logger;
        _endPoint = null;
    }

    public async ValueTask DisconnectAsync(CancellationToken token = default)
    {
        if (!Socket.Connected)
        {
            return;
        }

        _logger.Verbose("Отключаю соединение");
        try
        {
            await Socket.DisconnectAsync(true, token);
            _logger.Verbose("Соединение отключено");
        }
        catch (SocketException se) when (se.SocketErrorCode is SocketError.NotConnected)
        {
            _logger.Verbose("Запрошено разъединение с неподключенным узлом");
        }
    }

    public async ValueTask<bool> ConnectAsync(CancellationToken token = default)
    {
        if (_endPoint is null)
        {
            throw new InvalidOperationException("Невозможно подключиться. Адрес для подключения не указан");
        }
        
        if (Socket.Connected)
        {
            _logger.Verbose("Отключаю соединение");
            await Socket.DisconnectAsync(true, token);
        }
        
        return await ConnectAsyncCore(token);
    }

    private async ValueTask<bool> ConnectAsyncCore(CancellationToken token)
    {
        try
        {
            await Socket.ConnectAsync(_endPoint!, token);
            return true;
        }
        catch (SocketException se) when (se.SocketErrorCode is 
                                             SocketError.NetworkDown or
                                             SocketError.NetworkReset or 
                                             SocketError.NetworkUnreachable or 
                                             SocketError.HostNotFound or 
                                             SocketError.HostUnreachable or 
                                             SocketError.HostDown)
        {
            return false;
        }
    }
}