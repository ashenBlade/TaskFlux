using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Raft.Core;
using Serilog;

namespace Raft.Peer;

public class RaftTcpSocket: ISocket
{
    private readonly string _host;
    private readonly int _port;
    private readonly PeerId _nodeId;
    private readonly ILogger _logger;
    private readonly Socket _socket;
    private volatile ConnectionState _state = ConnectionState.NotConnected;
    private readonly SemaphoreSlim _lock = new(1);
    
    public RaftTcpSocket(string host, int port, PeerId nodeId, ILogger logger)
    {
        _host = host;
        _port = port;
        _nodeId = nodeId;
        _logger = logger;
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    }
    
    public async Task SendAsync(byte[] payload, CancellationToken token = default)
    {
        await CheckConnectionAsync(token);
        await _lock.WaitAsync(token);
        try
        {

            var left = payload.Length;
            var sent = await _socket.SendAsync(payload, SocketFlags.None);
            left -= sent;
            while (0 < left)
            {
                sent = await _socket.SendAsync(payload.AsMemory(sent), SocketFlags.None, token);
                left -= sent;
                sent += sent;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private async ValueTask CheckConnectionAsync(CancellationToken token)
    {
        if (_state is ConnectionState.Connected)
        {
            return;
        }

        if (_state is ConnectionState.Connecting)
        {
            await _lock.WaitAsync(token);
            if (_state is ConnectionState.Connected)
            {
                _lock.Release();
                return;
            }
        }

        await EstablishConnectionAsync(token);
    }

    private async Task EstablishConnectionAsync(CancellationToken token)
    {
        await _lock.WaitAsync(token);
        if (_state is ConnectionState.Connected)
        {
            return;
        }

        try
        {
            _logger.Debug("Делаю запрос подключения на хост {Host} и порт {Port}", _host, _port);
            await _socket.ConnectAsync(_host, _port, token);
            _logger.Debug("Соединение установлено. Делаю запрос авторизации");
            var connectionPacket = CreateConnectPacket();
            var sent = 0;
            do
            {
                sent += await _socket.SendAsync(connectionPacket.AsMemory(sent), SocketFlags.None, token);
            } while (sent != connectionPacket.Length);

            _logger.Debug("Запрос авторизации выполнен. Ожидаю ответа");
            var buffer = new byte[2];
            _ = await _socket.ReceiveAsync(buffer, SocketFlags.None, token);

            if (CheckSuccess(buffer))
            {
                _logger.Debug("Авторизация прошла успешно");
                _state = ConnectionState.Connected;
            }
            else
            {
                _logger.Debug("Авторизоваться не удалось");
                throw new AuthenticationException("Не удалось авторизоваться на узле");
            }
        }
        finally
        {
            _lock.Release();
        }

        bool CheckSuccess(byte[] data)
        {
            return data[0] is ( byte ) RequestType.Connect && data[1] != 0;
        }
        
        byte[] CreateConnectPacket()
        {
            var memory = new MemoryStream(5);
            var writer = new BinaryWriter(memory, Encoding.Default, true);
            writer.Write(( byte ) RequestType.Connect);
            writer.Write(_nodeId.Value);
            return memory.ToArray();
        }
    }


    public async Task<int> ReadAsync(byte[] buffer, CancellationToken token = default)
    {
        await CheckConnectionAsync(token);
        await _lock.WaitAsync(token);
        try
        {
            return await _socket.ReceiveAsync(buffer, SocketFlags.None, token);
            // int read;
            // var currentIndex = 0;
            // while ((read = await _socket.ReceiveAsync(buffer, SocketFlags.None, token)) > 0)
            // {
            //     var oldLeft = buffer.Length - currentIndex;
            //     currentIndex += read;
            //     if (read < oldLeft)
            //     {
            //         break;
            //     }
            // }
            //
            // return currentIndex;
        }
        finally
        {
            _lock.Release();
        }
    }

    public void Dispose()
    {
        _socket.Dispose();
        _lock.Dispose();
    }
}