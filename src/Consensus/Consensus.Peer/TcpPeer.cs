using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Peer;

public class TcpPeer : IPeer
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

    private async Task<AppendEntriesResponse?> SendAppendEntriesCoreAsync(
        AppendEntriesRequest request,
        CancellationToken token)
    {
        using var cts = CreateTimeoutCts(token, _requestTimeout);

        await _client.SendAsync(new AppendEntriesRequestPacket(request), cts.Token);

        var packet = await _client.ReceiveAsync(cts.Token);

        return packet.PacketType switch
               {
                   RaftPacketType.AppendEntriesResponse => ( ( AppendEntriesResponsePacket ) packet ).Response,
                   _ => throw new ArgumentException(
                            $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {packet.PacketType}")
               };
    }

    public async Task<AppendEntriesResponse?> SendAppendEntriesAsync(AppendEntriesRequest request,
                                                                     CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (_client.Socket.Connected)
        {
            try
            {
                return await SendAppendEntriesCoreAsync(request, token);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
        }

        if (await TryEstablishConnectionAsync(token))
        {
            try
            {
                return await SendAppendEntriesCoreAsync(request, token);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
        }

        return null;
    }

    private async Task<RequestVoteResponse?> SendRequestVoteCoreAsync(RequestVoteRequest request,
                                                                      CancellationToken token)
    {
        using var cts = CreateTimeoutCts(token, _requestTimeout);

        _logger.Debug("Делаю запрос RequestVote");
        await _client.SendAsync(new RequestVoteRequestPacket(request), cts.Token);

        _logger.Debug("Запрос отослан. Получаю ответ");
        var response = await _client.ReceiveAsync(cts.Token);

        _logger.Debug("Ответ получен: {@Response}", response);
        return response.PacketType switch
               {
                   RaftPacketType.RequestVoteResponse => ( ( RequestVoteResponsePacket ) response ).Response,
                   RaftPacketType.ConnectResponse     => null,
                   _ => throw new ArgumentException(
                            $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {response.PacketType}")
               };
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);
        Console.WriteLine($"SendRequestVote");

        if (_client.Socket.Connected)
        {
            try
            {
                return await SendRequestVoteCoreAsync(request, token);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
        }

        if (await TryEstablishConnectionAsync(token))
        {
            try
            {
                return await SendRequestVoteCoreAsync(request, token);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
        }

        return null;
    }

    public IEnumerable<InstallSnapshotResponse?> SendInstallSnapshot(InstallSnapshotRequest request,
                                                                     CancellationToken token)
    {
        _logger.Debug("Отправляю снапшот на узел {NodeId}", Id);
        // 1. Отправляем заголовок
        var requestPacket = new InstallSnapshotRequestPacket(request.Term, request.LeaderId,
            new LogEntryInfo(request.LastIncludedTerm, request.LastIncludedIndex));
        var requestResponse = SendPacketReturning(requestPacket, token);
        if (requestResponse is null)
        {
            if (!TryEstablishConnection(token))
            {
                yield return null;
                yield break;
            }

            requestResponse = SendPacketReturning(requestPacket, token);
            if (requestResponse is null)
            {
                yield return null;
                yield break;
            }
        }

        yield return GetInstallSnapshotResponse(requestResponse);

        // 2. Поочередно отправляем чанки
        foreach (var chunk in request.Snapshot.GetAllChunks(token))
        {
            var chunkResponse = SendPacketReturning(new InstallSnapshotChunkPacket(chunk), token);
            if (chunkResponse is null)
            {
                yield return null;
                yield break;
            }

            yield return GetInstallSnapshotResponse(chunkResponse);
        }

        yield break;

        static InstallSnapshotResponse GetInstallSnapshotResponse(RaftPacket packet)
        {
            if (packet is not {PacketType: RaftPacketType.InstallSnapshotResponse})
            {
                throw new Exception(
                    $"Неожиданный пакет получен от узла. Ожидался {RaftPacketType.InstallSnapshotResponse}. Получен: {packet.PacketType}");
            }

            if (packet is InstallSnapshotResponsePacket {CurrentTerm: var currentTerm})
            {
                return new InstallSnapshotResponse(currentTerm);
            }

            throw new Exception(
                $"Тип пакета {RaftPacketType.InstallSnapshotResponse}, но тип объекта не {nameof(InstallSnapshotResponsePacket)}");
        }
    }

    private RaftPacket? SendPacketReturning(RaftPacket request, CancellationToken token)
    {
        try
        {
            _client.Send(request, token);
            return _client.Receive(token);
        }
        catch (SocketException)
        {
            return null;
        }
        catch (IOException)
        {
            return null;
        }
    }

    private bool TryEstablishConnection(CancellationToken token)
    {
        _logger.Debug("Начинаю устанавливать соединение");
        var connected = _client.Connect(_endPoint);
        if (!connected)
        {
            return false;
        }

        using var cts = CreateTimeoutCts(token, _requestTimeout);
        try
        {
            _logger.Debug("Отправляю пакет авторизации");
            _client.Send(new ConnectRequestPacket(_currentNodeId), cts.Token);

            _logger.Debug("Начинаю получать ответ от узла");
            var packet = _client.Receive(cts.Token);

            switch (packet.PacketType)
            {
                case RaftPacketType.ConnectResponse:

                    var response = ( ConnectResponsePacket ) packet;
                    if (response.Success)
                    {
                        _logger.Debug("Авторизация прошла успшено");
                        return true;
                    }

                    throw new AuthenticationException("Ошибка при попытке авторизации на узле");

                case RaftPacketType.ConnectRequest:
                case RaftPacketType.RequestVoteRequest:
                case RaftPacketType.RequestVoteResponse:
                case RaftPacketType.AppendEntriesRequest:
                case RaftPacketType.AppendEntriesResponse:
                    throw new InvalidOperationException(
                        $"От узла пришел неожиданный ответ: PacketType: {packet.PacketType}");
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        catch (SocketException)
        {
            _client.Socket.Disconnect(true);
        }
        catch (IOException)
        {
            _client.Socket.Disconnect(true);
        }
        catch (OperationCanceledException)
        {
            _client.Socket.Disconnect(true);
        }

        return false;
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
            await _client.SendAsync(new ConnectRequestPacket(_currentNodeId), cts.Token);

            _logger.Debug("Начинаю получать ответ от узла");
            var packet = await _client.ReceiveAsync(cts.Token);

            switch (packet.PacketType)
            {
                case RaftPacketType.ConnectResponse:

                    var response = ( ConnectResponsePacket ) packet;
                    if (response.Success)
                    {
                        _logger.Debug("Авторизация прошла успшено");
                        return true;
                    }

                    throw new AuthenticationException("Ошибка при попытке авторизации на узле");

                case RaftPacketType.ConnectRequest:
                case RaftPacketType.RequestVoteRequest:
                case RaftPacketType.RequestVoteResponse:
                case RaftPacketType.AppendEntriesRequest:
                case RaftPacketType.AppendEntriesResponse:
                    throw new InvalidOperationException(
                        $"От узла пришел неожиданный ответ: PacketType: {packet.PacketType}");
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        catch (SocketException)
        {
            await _client.DisconnectAsync(token);
        }
        catch (IOException)
        {
            await _client.DisconnectAsync(token);
        }
        catch (OperationCanceledException)
        {
            await _client.DisconnectAsync(CancellationToken.None);
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