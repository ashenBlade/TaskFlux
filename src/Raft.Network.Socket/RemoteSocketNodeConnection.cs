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
        _logger.Verbose("Отключаю соединение");
        await Socket.DisconnectAsync(true, token);
        _logger.Verbose("Соедидение отключено");
    }

    public async ValueTask ConnectAsync(CancellationToken token = default)
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
        
        ConnectAsyncCore(token);
    }

    private void ConnectAsyncCore(CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            _logger.Verbose("Отправляю запрос соединения");
            using var cts = Create();
            var asyncResult = Socket.BeginConnect(_endPoint!, null, null);
            var success = asyncResult.AsyncWaitHandle.WaitOne(TimeSpan.FromMilliseconds(500));
            if (success)
            {
                return;
            }
                
            _logger.Verbose("Таймаут соединения превышен. Делаю повторную попытку соединения");
        }
        
        _logger.Verbose("Не удалось установить соединие. Токен был отменен");

        CancellationTokenSource Create()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            cts.CancelAfter(TimeSpan.FromMilliseconds(500));
            return cts;
        }
    }

    public void Dispose()
    {
        Socket.Close();
        Socket.Dispose();
    }
}