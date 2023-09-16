using System.ComponentModel;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Peer;

public class TcpPeer : IPeer
{
    private static BinaryPacketDeserializer Deserializer => BinaryPacketDeserializer.Instance;
    private readonly Socket _socket;
    private readonly EndPoint _endPoint;
    private readonly NodeId _currentNodeId;
    private readonly TimeSpan _requestTimeout;

    private readonly Lazy<NetworkStream> _lazy;
    private NetworkStream NetworkStream => _lazy.Value;

    private readonly ILogger _logger;
    public NodeId Id { get; }

    private TcpPeer(Socket socket,
                    EndPoint endPoint,
                    NodeId nodeId,
                    NodeId currentNodeId,
                    TimeSpan requestTimeout,
                    Lazy<NetworkStream> lazy,
                    ILogger logger)
    {
        Id = nodeId;
        _socket = socket;
        _endPoint = endPoint;
        _currentNodeId = currentNodeId;
        _requestTimeout = requestTimeout;
        _lazy = lazy;
        _logger = logger;
    }

    private async Task<AppendEntriesResponse?> SendAppendEntriesCoreAsync(
        AppendEntriesRequest request,
        CancellationToken token)
    {
        using var cts = CreateTimeoutCts(token, _requestTimeout);

        await SendPacketAsync(new AppendEntriesRequestPacket(request), cts.Token);

        var packet = await Deserializer.DeserializeAsync(NetworkStream, cts.Token);

        return packet.PacketType switch
               {
                   RaftPacketType.AppendEntriesResponse => ( ( AppendEntriesResponsePacket ) packet ).Response,
                   _ => throw new ArgumentException(
                            $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {packet.PacketType}")
               };
    }

    private ValueTask SendPacketAsync(RaftPacket packet, CancellationToken token)
    {
        return packet.SerializeAsync(NetworkStream, token);
    }

    private void SendPacket(RaftPacket packet)
    {
        packet.Serialize(NetworkStream);
    }

    public async Task<AppendEntriesResponse?> SendAppendEntriesAsync(AppendEntriesRequest request,
                                                                     CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (_socket.Connected)
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

    public AppendEntriesResponse? SendAppendEntries(AppendEntriesRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var packet = new AppendEntriesRequestPacket(request);

        if (_socket.Connected)
        {
            try
            {
                return SendAppendEntriesCore(packet);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
        }

        if (TryEstablishConnection())
        {
            try
            {
                return SendAppendEntriesCore(packet);
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

    private AppendEntriesResponse SendAppendEntriesCore(AppendEntriesRequestPacket requestPacket)
    {
        requestPacket.Serialize(NetworkStream);

        var responsePacket = Deserializer.Deserialize(NetworkStream);

        return responsePacket.PacketType switch
               {
                   RaftPacketType.AppendEntriesResponse => ( ( AppendEntriesResponsePacket ) responsePacket ).Response,
                   _ => throw new ArgumentException(
                            $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {responsePacket.PacketType}")
               };
    }

    private RequestVoteResponse SendRequestVoteCore(RequestVoteRequestPacket requestPacket)
    {
        requestPacket.Serialize(NetworkStream);

        var responsePacket = Deserializer.Deserialize(NetworkStream);

        return responsePacket.PacketType switch
               {
                   RaftPacketType.RequestVoteResponse => ( ( RequestVoteResponsePacket ) responsePacket ).Response,
                   _ => throw new ArgumentException(
                            $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {responsePacket.PacketType}")
               };
    }

    private async Task<RequestVoteResponse?> SendRequestVoteCoreAsync(RequestVoteRequestPacket requestPacket,
                                                                      CancellationToken token)
    {
        await requestPacket.SerializeAsync(NetworkStream, token);
        var response = await ReceivePacketAsync(token);
        return response.PacketType switch
               {
                   RaftPacketType.RequestVoteResponse => ( ( RequestVoteResponsePacket ) response ).Response,
                   _ => throw new ArgumentException(
                            $"От узла пришел неожиданный ответ. Ожидался AppendEntriesResponse. Пришел: {response.PacketType}")
               };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<RaftPacket> ReceivePacketAsync(CancellationToken token)
    {
        return Deserializer.DeserializeAsync(NetworkStream, token);
    }

    public async Task<RequestVoteResponse?> SendRequestVoteAsync(RequestVoteRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        var packet = new RequestVoteRequestPacket(request);

        if (_socket.Connected)
        {
            try
            {
                return await SendRequestVoteCoreAsync(packet, token);
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
                return await SendRequestVoteCoreAsync(packet, token);
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

    public RequestVoteResponse? SendRequestVote(RequestVoteRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var packet = new RequestVoteRequestPacket(request);

        if (_socket.Connected)
        {
            try
            {
                return SendRequestVoteCore(packet);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
        }

        if (TryEstablishConnection())
        {
            try
            {
                return SendRequestVoteCore(packet);
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
        // Проверка через Connected плохая, но неплохо для начала.
        // Если соединение все же было разорвано, то заметим это при отправке заголовка
        if (!( _socket.Connected || TryEstablishConnection() ))
        {
            return new InstallSnapshotResponse?[] {null};
        }

        return SendInstallSnapshotCore(request, token);
    }

    private IEnumerable<InstallSnapshotResponse?> SendInstallSnapshotCore(InstallSnapshotRequest request,
                                                                          CancellationToken token)
    {
        _logger.Debug("Отправляю снапшот на узел {NodeId}", Id);

        // 1. Отправляем заголовок
        var requestPacket = new InstallSnapshotRequestPacket(request.Term, request.LeaderId, request.LastEntry);
        _logger.Debug("Посылаю InstallSnapshotChunk пакет");
        var requestResponse = SendPacketReturning(requestPacket);
        if (requestResponse is null)
        {
            if (!TryEstablishConnection())
            {
                yield return null;
                yield break;
            }

            requestResponse = SendPacketReturning(requestPacket);
            if (requestResponse is null)
            {
                yield return null;
                yield break;
            }
        }

        var response = GetInstallSnapshotResponse(requestResponse);
        _logger.Debug("Ответ получен. Терм узла: {Term}", response.CurrentTerm);
        yield return response;

        // 2. Поочередно отправляем чанки
        var chunkNumber = 1;
        foreach (var chunk in request.Snapshot.GetAllChunks(token))
        {
            _logger.Debug("Отправляю {Number} чанк данных", chunkNumber);
            var chunkResponse = SendPacketReturning(new InstallSnapshotChunkPacket(chunk));
            if (chunkResponse is null)
            {
                _logger.Debug("Во время отправки чанка данных соединение было разорвано");
                yield return null;
                yield break;
            }

            yield return GetInstallSnapshotResponse(chunkResponse);
        }

        // Отправляем последний пустой пакет - окончание передачи
        {
            _logger.Debug("Отправка чанков закончена. Отправляю последний пакет");
            var chunkResponse = SendPacketReturning(new InstallSnapshotChunkPacket(Memory<byte>.Empty));
            if (chunkResponse is null)
            {
                _logger.Debug("Во время отправки последнего пакета соединение было разорвано");
                yield return null;
                yield break;
            }

            yield return GetInstallSnapshotResponse(chunkResponse);
        }

        // Это для явного разделения конца метода и локальной функции
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

    private RaftPacket? SendPacketReturning(RaftPacket request)
    {
        try
        {
            request.Serialize(NetworkStream);
            return Deserializer.Deserialize(NetworkStream);
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

    private bool TryEstablishConnection()
    {
        if (_socket.Connected)
        {
            return true;
        }

        _logger.Debug("Начинаю устанавливать соединение");
        try
        {
            _socket.Connect(_endPoint);

            _logger.Debug("Отправляю пакет авторизации");
            SendPacket(new ConnectRequestPacket(_currentNodeId));
            _logger.Debug("Начинаю получать ответ подключения от узла");
            var packet = Deserializer.Deserialize(NetworkStream);

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
                case RaftPacketType.InstallSnapshotRequest:
                case RaftPacketType.InstallSnapshotChunk:
                case RaftPacketType.InstallSnapshotResponse:
                    throw new InvalidOperationException(
                        $"От узла пришел неожиданный ответ: PacketType: {packet.PacketType}");
                default:
                    throw new InvalidEnumArgumentException(nameof(packet.PacketType), ( int ) packet.PacketType,
                        typeof(RaftPacketType));
            }
        }
        catch (SocketException)
        {
        }
        catch (IOException)
        {
        }

        return false;
    }

    private async ValueTask<bool> TryEstablishConnectionAsync(CancellationToken token = default)
    {
        _logger.Debug("Начинаю устанавливать соединение");
        try
        {
            await _socket.ConnectAsync(_endPoint, token);
            _logger.Debug("Отправляю пакет авторизации");
            var connectRequestPacket = new ConnectRequestPacket(_currentNodeId);
            await SendPacketAsync(connectRequestPacket, token);
            _logger.Debug("Начинаю получать ответ от узла");
            var packet = await ReceivePacketAsync(token);

            switch (packet.PacketType)
            {
                case RaftPacketType.ConnectResponse:

                    var response = ( ConnectResponsePacket ) packet;
                    if (response.Success)
                    {
                        _logger.Debug("Авторизация прошла успешно");
                        return true;
                    }

                    throw new AuthenticationException("Ошибка при попытке авторизации на узле");

                case RaftPacketType.ConnectRequest:
                case RaftPacketType.RequestVoteRequest:
                case RaftPacketType.RequestVoteResponse:
                case RaftPacketType.AppendEntriesRequest:
                case RaftPacketType.AppendEntriesResponse:
                case RaftPacketType.InstallSnapshotRequest:
                case RaftPacketType.InstallSnapshotChunk:
                case RaftPacketType.InstallSnapshotResponse:
                    throw new InvalidOperationException(
                        $"От узла пришел неожиданный ответ: PacketType: {packet.PacketType}");
                default:
                    throw new InvalidEnumArgumentException(nameof(packet.PacketType), ( int ) packet.PacketType,
                        typeof(RaftPacketType));
            }
        }
        catch (SocketException)
        {
        }
        catch (IOException)
        {
        }

        return false;
    }

    private static CancellationTokenSource CreateTimeoutCts(CancellationToken token, TimeSpan timeout)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(timeout);
        return cts;
    }

    public static TcpPeer Create(NodeId currentNodeId,
                                 NodeId nodeId,
                                 EndPoint endPoint,
                                 TimeSpan requestTimeout,
                                 ILogger logger)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        // Убираем алгоритм Нагла - отправляем сразу
        socket.NoDelay = true;

        socket.SendTimeout = ( int ) requestTimeout.TotalMilliseconds;
        socket.ReceiveTimeout = ( int ) requestTimeout.TotalMilliseconds;

        var lazyStream = new Lazy<NetworkStream>(() => new NetworkStream(socket));

        return new TcpPeer(socket, endPoint, nodeId, currentNodeId, requestTimeout, lazyStream, logger);
    }
}