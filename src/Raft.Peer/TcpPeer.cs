using System.Net.Sockets;
using System.Security.Authentication;
using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Network;
using Raft.Network.Packets;
using Raft.Network.Socket;
using Serilog;

namespace Raft.Peer;

public class TcpPeer: IPeer
{
    private readonly IRemoteNodeConnection _connection;
    private readonly NodeId _currentNodeId;

    private readonly ILogger _logger;
    public NodeId Id { get; }

    public TcpPeer(IRemoteNodeConnection connection, NodeId nodeId, NodeId currentNodeId, ILogger logger)
    {
        Id = nodeId;
        _connection = connection;
        _currentNodeId = currentNodeId;
        _logger = logger;
    }

    public async Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        while (token.IsCancellationRequested is false)
        {
            _logger.Debug("Делаю запрос AppendEntries");
            var success = await _connection.SendAsync(new AppendEntriesRequestPacket(request), token);

            if (!success)
            {
                _logger.Debug("SendAsync вернул false. Переподключаюсь");
                await EstablishConnectionAsync(token);
                continue;
            }
        
            _logger.Debug("Запрос AppendEntries отослан. Получаю ответ");
            var packet = await _connection.ReceiveAsync(token);

            if (packet is null)
            {
                _logger.Debug("Узел вернул null. Делаю повторное подключение");
                await EstablishConnectionAsync(token);
                continue;
            }
        
            _logger.Debug("Ответ получен: {Response}", packet);
            return packet.PacketType switch
                   {
                       PacketType.AppendEntriesResponse => ( ( AppendEntriesResponsePacket ) packet ).Response,
                       _ => throw new ArgumentException($"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {packet.PacketType}")
                   };
        }

        return null;
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        while (token.IsCancellationRequested is false)
        {
            _logger.Debug("Делаю запрос RequestVote");
            var success = await _connection.SendAsync(new RequestVoteRequestPacket(request), token);
            if (!success)
            {
                _logger.Verbose("Ошибка во время отправки запроса. Устанавливаю соединение");
                await EstablishConnectionAsync(token);
                continue;
            }
            
            _logger.Debug("Запрос отослан. Получаю ответ");
            var response = await _connection.ReceiveAsync(token);

            if (response is null)
            {
                _logger.Verbose("Ошибка во время получения ответа от узла. Устанавливаю соединение");
                await EstablishConnectionAsync(token);
                continue;
            }
            
            _logger.Debug("Ответ получен: {Response}", response);
            return response.PacketType switch
                   {
                       PacketType.RequestVoteResponse => ( ( RequestVoteResponsePacket ) response ).Response,
                       _ => throw new ArgumentException(
                                $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {response.PacketType}")
                   };
        }

        return null;
    }

    private async Task EstablishConnectionAsync(CancellationToken token = default)
    {
        _logger.Debug("Начинаю устанавливать соединение");
        while (token.IsCancellationRequested is false)
        {
            _logger.Debug("Подключаюсь");
            await _connection.ConnectAsync(token);
            
            try
            {
                _logger.Debug("Отправляю пакет авторизации");
                var success = await _connection.SendAsync(new ConnectRequestPacket(_currentNodeId), token);
                if (!success)
                {
                    _logger.Debug("Не удалось отправить пакет авторизации. Делаю повторную попытку");
                    await _connection.DisconnectAsync(token);
                    continue;
                }

                _logger.Debug("Начинаю получать ответ от узла");
                var packet = await _connection.ReceiveAsync(token);
                if (packet is null)
                {
                    _logger.Debug("От узла вернулся null. Делаю повторную попытку");
                    await _connection.DisconnectAsync(token);
                    continue;
                }
            
                
                switch (packet.PacketType)
                {
                    case PacketType.ConnectResponse:
                        
                        var response = ( ConnectResponsePacket ) packet;
                        if (response.Success)
                        {
                            _logger.Debug("Авторизация прошла успшено");
                            return;
                        }

                        throw new AuthenticationException("Ошибка при попытке авторизации на узле");

                    case PacketType.ConnectRequest:
                    case PacketType.RequestVoteRequest:
                    case PacketType.RequestVoteResponse:
                    case PacketType.AppendEntriesRequest:
                    case PacketType.AppendEntriesResponse:
                        throw new InvalidOperationException($"От узла пришел неожиданный ответ: PacketType: {packet.PacketType}");
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (SocketException)
            {
                await _connection.DisconnectAsync(token);
            }
        }
    }
}