using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Network;
using Raft.Network.Packets;
using Serilog;

namespace Raft.Peer;

public class TcpPeer: IPeer
{
    private readonly EndPoint _endPoint;
    private readonly NodeId _currentNodeId;
    private readonly TimeSpan _connectionTimeout;
    private readonly TimeSpan _requestTimeout;
    private readonly PacketClient _client;
    private readonly ILogger _logger;
    public NodeId Id { get; }

    public TcpPeer(PacketClient client,
                   EndPoint endPoint,
                   NodeId nodeId,
                   NodeId currentNodeId,
                   TimeSpan connectionTimeout,
                   TimeSpan requestTimeout,
                   ILogger logger)
    {
        Id = nodeId;
        _endPoint = endPoint;
        _currentNodeId = currentNodeId;
        _connectionTimeout = connectionTimeout;
        _requestTimeout = requestTimeout;
        _logger = logger;
        _client = client;
    }

    public async Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!await CheckConnectionAsync(token))
        {
            return null;
        }
        
        using var cts = CreateTimeoutCts(token, _requestTimeout);
        try
        {
            var success = await _client.SendAsync(new AppendEntriesRequestPacket(request), cts.Token);
            if (!success)
            {
                _logger.Debug("SendAsync вернул false. Соединение было закрыто");
                return null;
            }
        
            var packet = await _client.ReceiveAsync(cts.Token);
            if (packet is null)
            {
                _logger.Debug("Узел вернул null. Соединение было закрыто");
                return null;
            }
            
            return packet.PacketType switch
                   {
                       PacketType.AppendEntriesResponse => ( ( AppendEntriesResponsePacket ) packet ).Response,
                       _ => throw new ArgumentException(
                                $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {packet.PacketType}")
                   };
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested || token.IsCancellationRequested)
        {
            return null;
        }
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);
        
        if (!await CheckConnectionAsync(token))
        {
            return null;
        }
        
        using var cts = CreateTimeoutCts(token, _requestTimeout);

        try
        {
            _logger.Debug("Делаю запрос RequestVote");
            var success = await _client.SendAsync(new RequestVoteRequestPacket(request), cts.Token);
            if (!success)
            {
                _logger.Verbose("Ошибка во время отправки запроса. Соединение было разорвано");
                return null;
            }
            
            _logger.Debug("Запрос отослан. Получаю ответ");
            var response = await _client.ReceiveAsync(cts.Token);
            if (response is null)
            {
                _logger.Verbose("Ошибка во время получения ответа от узла. Соединение было разорвано");
                return null;
            }
            
            _logger.Debug("Ответ получен: {Response}", response);
            return response.PacketType switch
                   {
                       PacketType.RequestVoteResponse => ( ( RequestVoteResponsePacket ) response ).Response,
                       PacketType.ConnectResponse     => null,
                       _ => throw new ArgumentException(
                                $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {response.PacketType}")
                   };
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested || token.IsCancellationRequested)
        {
            return null;
        }
    }

    private async ValueTask<bool> CheckConnectionAsync(CancellationToken token = default)
    {
        if (_client.Socket.Connected)
        {
            return true;
        }

        return await TryEstablishConnectionAsync(token);
    }

    private async ValueTask<bool> TryEstablishConnectionAsync(CancellationToken token = default)
    {
        _logger.Debug("Начинаю устанавливать соединение");
        var connected = await _client.ConnectAsync(_endPoint, _connectionTimeout, token);
        if (!connected)
        {
            return false;
        }

        using var cts = CreateTimeoutCts(token, _requestTimeout);
        try
        {
            _logger.Debug("Отправляю пакет авторизации");
            var success = await _client.SendAsync(new ConnectRequestPacket(_currentNodeId), cts.Token);
            if (!success)
            {
                _logger.Debug("Не удалось отправить пакет авторизации. Делаю повторную попытку");
                await _client.DisconnectAsync(token);
                return false;
            }

            _logger.Debug("Начинаю получать ответ от узла");
            var packet = await _client.ReceiveAsync(cts.Token);
            if (packet is null)
            {
                _logger.Debug("От узла вернулся null. Делаю повторную попытку");
                await _client.DisconnectAsync(cts.Token);
                return false;
            }

            switch (packet.PacketType)
            {
                case PacketType.ConnectResponse:

                    var response = ( ConnectResponsePacket ) packet;
                    if (response.Success)
                    {
                        _logger.Debug("Авторизация прошла успшено");
                        return true;
                    }

                    throw new AuthenticationException("Ошибка при попытке авторизации на узле");

                case PacketType.ConnectRequest:
                case PacketType.RequestVoteRequest:
                case PacketType.RequestVoteResponse:
                case PacketType.AppendEntriesRequest:
                case PacketType.AppendEntriesResponse:
                    throw new InvalidOperationException(
                        $"От узла пришел неожиданный ответ: PacketType: {packet.PacketType}");
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
        {
            token.ThrowIfCancellationRequested();
        }
        catch (SocketException)
        {
            await _client.DisconnectAsync(token);
        }
        

        return false;
    }

    private static CancellationTokenSource CreateTimeoutCts(CancellationToken token, TimeSpan timeout)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(timeout);
        return cts;
    }
}