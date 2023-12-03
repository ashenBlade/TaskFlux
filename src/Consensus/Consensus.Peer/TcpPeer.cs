using System.ComponentModel;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Peer.Exceptions;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Serilog;
using TaskFlux.Models;

namespace Consensus.Peer;

public class TcpPeer : IPeer
{
    private static BinaryPacketDeserializer Deserializer => BinaryPacketDeserializer.Instance;
    private Socket _socket;
    private readonly EndPoint _endPoint;
    private readonly NodeId _currentNodeId;
    private readonly TimeSpan _requestTimeout;

    private Lazy<NetworkStream> _lazy;
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

    public async Task<AppendEntriesResponse?> SendAppendEntriesAsync(AppendEntriesRequest request,
                                                                     CancellationToken token)
    {
        var response = await SendPacketReconnectingCoreAsync(new AppendEntriesRequestPacket(request), token);
        if (response is null)
        {
            return null;
        }

        switch (response.PacketType)
        {
            case RaftPacketType.AppendEntriesResponse:
                var appendEntriesResponsePacket = ( AppendEntriesResponsePacket ) response;
                return appendEntriesResponsePacket.Response;
            case RaftPacketType.AppendEntriesRequest:
            case RaftPacketType.ConnectRequest:
            case RaftPacketType.ConnectResponse:
            case RaftPacketType.RequestVoteRequest:
            case RaftPacketType.RequestVoteResponse:
            case RaftPacketType.InstallSnapshotRequest:
            case RaftPacketType.InstallSnapshotChunk:
            case RaftPacketType.InstallSnapshotResponse:
            case RaftPacketType.RetransmitRequest:
                throw new UnexpectedPacketException(response, RaftPacketType.AppendEntriesResponse);
        }

        throw new InvalidEnumArgumentException(nameof(response.PacketType), ( int ) response.PacketType,
            typeof(RaftPacketType));
    }

    public AppendEntriesResponse? SendAppendEntries(AppendEntriesRequest request)
    {
        var response = SendPacketReconnectingCore(new AppendEntriesRequestPacket(request));
        if (response is null)
        {
            return null;
        }

        switch (response.PacketType)
        {
            case RaftPacketType.AppendEntriesResponse:
                var appendEntriesResponsePacket = ( AppendEntriesResponsePacket ) response;
                return appendEntriesResponsePacket.Response;

            case RaftPacketType.AppendEntriesRequest:
            case RaftPacketType.ConnectRequest:
            case RaftPacketType.ConnectResponse:
            case RaftPacketType.RequestVoteRequest:
            case RaftPacketType.RequestVoteResponse:
            case RaftPacketType.InstallSnapshotRequest:
            case RaftPacketType.InstallSnapshotChunk:
            case RaftPacketType.InstallSnapshotResponse:
            case RaftPacketType.RetransmitRequest:
                throw new UnexpectedPacketException(response, RaftPacketType.AppendEntriesResponse);
        }

        throw new InvalidEnumArgumentException(nameof(response.PacketType), ( int ) response.PacketType,
            typeof(RaftPacketType));
    }

    /// <summary>
    /// Базовый метод для отправки переданного пакета с получением ответа.
    /// Если во время отправки соединение было потеряно, то делается попытка повторного установления соединения
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <param name="token">Токен отмены, переданный вызывающей стороной</param>
    /// <returns>Полученный пакет или <c>null</c> если соединение было разорвано и пакет не был получен</returns>
    private async Task<RaftPacket?> SendPacketReconnectingCoreAsync(RaftPacket packet, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(packet);

        if (_socket.Connected)
        {
            var response = await SendPacketReturningAsync(packet, token);
            if (response is not null)
            {
                return response;
            }
        }

        if (await TryEstablishConnectionAsync(token))
        {
            return await SendPacketReturningAsync(packet, token);
        }

        return null;
    }

    /// <summary>
    /// Отправить пакет с учетом повторного переподключения при потере соединения
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <returns>Полученный пакет или <c>null</c>, если соедение потеряно</returns>
    private RaftPacket? SendPacketReconnectingCore(RaftPacket packet)
    {
        ArgumentNullException.ThrowIfNull(packet);

        if (_socket.Connected)
        {
            var response = SendPacketReturning(packet);
            if (response is not null)
            {
                return response;
            }
        }

        if (TryEstablishConnection())
        {
            return SendPacketReturning(packet);
        }

        return null;
    }

    public async Task<RequestVoteResponse?> SendRequestVoteAsync(RequestVoteRequest request, CancellationToken token)
    {
        var response = await SendPacketReconnectingCoreAsync(new RequestVoteRequestPacket(request), token);
        if (response is null)
        {
            return null;
        }

        switch (response.PacketType)
        {
            case RaftPacketType.RequestVoteResponse:
                var requestVoteResponsePacket = ( RequestVoteResponsePacket ) response;
                return requestVoteResponsePacket.Response;

            case RaftPacketType.AppendEntriesRequest:
            case RaftPacketType.AppendEntriesResponse:
            case RaftPacketType.ConnectRequest:
            case RaftPacketType.ConnectResponse:
            case RaftPacketType.RequestVoteRequest:
            case RaftPacketType.InstallSnapshotRequest:
            case RaftPacketType.InstallSnapshotChunk:
            case RaftPacketType.InstallSnapshotResponse:
            case RaftPacketType.RetransmitRequest:
                throw new UnexpectedPacketException(response, RaftPacketType.RequestVoteResponse);
        }

        throw new InvalidEnumArgumentException(nameof(response.PacketType), ( int ) response.PacketType,
            typeof(RaftPacketType));
    }

    public RequestVoteResponse? SendRequestVote(RequestVoteRequest request)
    {
        var response = SendPacketReconnectingCore(new RequestVoteRequestPacket(request));
        if (response is null)
        {
            return null;
        }

        switch (response.PacketType)
        {
            case RaftPacketType.RequestVoteResponse:
                var requestVoteResponsePacket = ( RequestVoteResponsePacket ) response;
                return requestVoteResponsePacket.Response;

            case RaftPacketType.AppendEntriesRequest:
            case RaftPacketType.AppendEntriesResponse:
            case RaftPacketType.ConnectRequest:
            case RaftPacketType.ConnectResponse:
            case RaftPacketType.RequestVoteRequest:
            case RaftPacketType.InstallSnapshotRequest:
            case RaftPacketType.InstallSnapshotChunk:
            case RaftPacketType.InstallSnapshotResponse:
            case RaftPacketType.RetransmitRequest:
                throw new UnexpectedPacketException(response, RaftPacketType.RequestVoteResponse);
        }

        throw new InvalidEnumArgumentException(nameof(response.PacketType), ( int ) response.PacketType,
            typeof(RaftPacketType));
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
        token.ThrowIfCancellationRequested();
        _logger.Debug("Отправляю снапшот на узел {NodeId}", Id);

        // 1. Отправляем заголовок
        var headerPacket = new InstallSnapshotRequestPacket(request.Term, request.LeaderId, request.LastEntry);
        _logger.Debug("Посылаю InstallSnapshotChunk пакет");

        // Попытку повторного установления соединения делаем только один раз - при отправке заголовка.
        // Если возникла ошибка, то полностью снапшот отправим в другом запросе
        var headerResponse = SendPacketReconnectingCore(headerPacket);

        if (headerResponse is null)
        {
            // Соединение все еще не удалось отправить
            yield return null;
            yield break;
        }

        // RetransmitRequest получить не можем - отправляем заголовок
        var response = GetInstallSnapshotResponse(headerResponse);
        _logger.Debug("Ответ получен. Терм узла: {Term}", response.CurrentTerm);
        yield return response;

        // 2. Поочередно отправляем чанки
        var chunkNumber = 1;
        foreach (var chunk in request.Snapshot.GetAllChunks(token))
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                _logger.Debug("Отправляю {Number} чанк данных", chunkNumber);
                var chunkResponse = SendPacketReturning(new InstallSnapshotChunkPacket(chunk));
                if (chunkResponse is null)
                {
                    _logger.Debug("Во время отправки чанка данных соединение было разорвано");
                    yield return null;
                    yield break;
                }

                yield return GetInstallSnapshotResponse(chunkResponse);

                // Делаем повторную попытку отправки - целостность была нарушена
            }
        }

        // Отправляем последний пустой пакет - окончание передачи

        _logger.Debug("Отправка чанков закончена. Отправляю последний пакет");
        var lastChunkResponse = SendPacketReturning(new InstallSnapshotChunkPacket(Memory<byte>.Empty));
        if (lastChunkResponse is null)
        {
            _logger.Debug("Во время отправки последнего пакета соединение было разорвано");
            yield return null;
            yield break;
        }

        yield return GetInstallSnapshotResponse(lastChunkResponse);

        // Это для явного разделения конца метода и локальной функции
        yield break;

        static InstallSnapshotResponse GetInstallSnapshotResponse(RaftPacket packet)
        {
            if (packet.PacketType is RaftPacketType.InstallSnapshotResponse)
            {
                var installSnapshotResponsePacket = ( InstallSnapshotResponsePacket ) packet;
                return new InstallSnapshotResponse(installSnapshotResponsePacket.CurrentTerm);
            }

            throw new UnexpectedPacketException(packet, RaftPacketType.InstallSnapshotResponse);
        }
    }


    /// <summary>
    /// Метод для однократной отправки пакета с учетом таймаутов и RetransmitRequest'ом.
    /// Если соединение теряется, то возвращается null (без попытки переподклчения)
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <returns>Полученный пакет, или <c>null</c> если соединение потеряно</returns>
    private RaftPacket? SendPacketReturning(RaftPacket packet)
    {
        while (true)
        {
            // TODO: SocketException TimedOut
            packet.Serialize(NetworkStream);

            try
            {
                var response = Deserializer.Deserialize(NetworkStream);
                if (response.PacketType is RaftPacketType.RetransmitRequest)
                {
                    // Повторно отправляем запрос
                    continue;
                }

                return response;
            }
            catch (IOException io)
                when (io.GetBaseException() is SocketException
                                               {
                                                   SocketErrorCode: SocketError.TimedOut
                                               })
            {
                _logger.Warning(
                    "Таймаут ожидания в {TimeoutMs}мс при отправке пакета {PacketType} превышен. Делаю повторную отправку",
                    _requestTimeout.TotalMilliseconds, packet.PacketType);
            }
            catch (IOException)
            {
                return null;
            }
            catch (SocketException)
            {
                return null;
            }
        }
    }

    /// <summary>
    /// Метод для однократной отправки пакета с учетом таймаутов и RetransmitRequest'ом.
    /// Если соединение теряется, то возвращается null (без попытки переподклчения)
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный пакет, или <c>null</c> если соединение потеряно</returns>
    private async Task<RaftPacket?> SendPacketReturningAsync(RaftPacket packet, CancellationToken token)
    {
        // Цикл нужен для повторных отправок, если:
        //  1. Получили RetransmitRequest
        //  2. Превышен таймаут ожидания ответа 
        while (true)
        {
            try
            {
                await packet.SerializeAsync(NetworkStream, token);
                using var cts = CreateRequestTimeoutCts(token);
                try
                {
                    var response = await Deserializer.DeserializeAsync(NetworkStream, token);
                    if (response.PacketType is RaftPacketType.RetransmitRequest)
                    {
                        // Нужно повторить отправку пакета - заходим на повторный круг цикла
                        continue;
                    }

                    return response;
                }
                catch (OperationCanceledException)
                    when (cts.IsCancellationRequested     // Превышен таймаут запроса
                       && !token.IsCancellationRequested) // А не переданный токен отменен
                {
                    // Заходим на второй круг
                }
            }
            catch (IOException)
            {
                return null;
            }
            catch (SocketException)
            {
                return null;
            }
        }
    }

    private bool TryEstablishConnection()
    {
        if (_socket.Connected)
        {
            // Так лучше не проверять, и надо бы использовать Poll,
            // но если соединение действительно было разорвано, 
            // то мы об этом узнаем и сделаем повторную попытку установления соединения
            return true;
        }

        _logger.Debug("Начинаю устанавливать соединение");

        try
        {
            return ConnectCore();
        }
        catch (InvalidOperationException)
        {
        }

        // Сервер очень быстро разорвал соединение (ungracefully)
        // Сделаем повторную попытку

        UpdateSocketState();

        try
        {
            return ConnectCore();
        }
        catch (InvalidOperationException)
        {
        }

        UpdateSocketState();

        return false;

        bool ConnectCore()
        {
            try
            {
                _socket.Connect(_endPoint);
            }
            catch (SocketException)
            {
                return false;
            }
            catch (IOException)
            {
                return false;
            }

            _logger.Debug("Делаю запрос авторизации");
            var response = SendPacketReturning(new ConnectRequestPacket(_currentNodeId));
            if (response is null)
            {
                return false;
            }

            switch (response.PacketType)
            {
                case RaftPacketType.ConnectResponse:

                    var connectResponsePacket = ( ConnectResponsePacket ) response;
                    if (connectResponsePacket.Success)
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
                case RaftPacketType.RetransmitRequest:
                    throw new InvalidOperationException(
                        $"От узла пришел неожиданный ответ: PacketType: {response.PacketType}");
                default:
                    throw new InvalidEnumArgumentException(nameof(response.PacketType), ( int ) response.PacketType,
                        typeof(RaftPacketType));
            }
        }
    }

    /// <summary>
    /// Метод для замены старого сокета и NetworkStream, когда соединение было разорвано.
    /// </summary>
    private void UpdateSocketState()
    {
        var newSocket = CreateSocket(_requestTimeout);
        var newLazy = new Lazy<NetworkStream>(() => new NetworkStream(newSocket));
        _socket.Close();
        _socket = newSocket;
        _lazy = newLazy;
    }

    /// <summary>
    /// Попытаться установить соединение.
    /// Производится одна попытка установления соединения - без повторов
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns><c>true</c> - соединение установлено успешно, <c>false</c> - соединение установить не удалось</returns>
    /// <exception cref="AuthenticationException">Ошибка во время аутентификации</exception>
    /// <exception cref="UnexpectedPacketException">От узла получен неожиданный пакет</exception>
    private async ValueTask<bool> TryEstablishConnectionAsync(CancellationToken token = default)
    {
        _logger.Debug("Начинаю устанавливать соединение");
        try
        {
            return await ConnectAsyncCore(token);
        }
        catch (InvalidOperationException)
        {
        }
        catch (SocketException)
        {
        }
        catch (IOException)
        {
        }

        return false;

        async ValueTask<bool> ConnectAsyncCore(CancellationToken cancellationToken)
        {
            await _socket.ConnectAsync(_endPoint, cancellationToken);
            _logger.Debug("Отправляю пакет авторизации");
            var connectRequestPacket = new ConnectRequestPacket(_currentNodeId);
            _logger.Debug("Начинаю получать ответ от узла");
            var packet = await SendPacketReturningAsync(connectRequestPacket, token);
            if (packet is null)
            {
                return false;
            }

            switch (packet.PacketType)
            {
                case RaftPacketType.ConnectResponse:

                    var connectResponsePacket = ( ConnectResponsePacket ) packet;
                    if (!connectResponsePacket.Success)
                    {
                        throw new AuthenticationException("Ошибка при попытке авторизации на узле");
                    }

                    _logger.Debug("Авторизация прошла успешно");
                    return true;

                case RaftPacketType.ConnectRequest:
                case RaftPacketType.RequestVoteRequest:
                case RaftPacketType.RequestVoteResponse:
                case RaftPacketType.AppendEntriesRequest:
                case RaftPacketType.AppendEntriesResponse:
                case RaftPacketType.InstallSnapshotRequest:
                case RaftPacketType.InstallSnapshotChunk:
                case RaftPacketType.RetransmitRequest:
                case RaftPacketType.InstallSnapshotResponse:
                    throw new UnexpectedPacketException(packet, RaftPacketType.ConnectResponse);
            }

            throw new InvalidEnumArgumentException(nameof(packet.PacketType), ( int ) packet.PacketType,
                typeof(RaftPacketType));
        }
    }

    private CancellationTokenSource CreateRequestTimeoutCts(CancellationToken token)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(_requestTimeout);
        return cts;
    }

    private static Socket CreateSocket(TimeSpan requestTimeout)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        // Убираем алгоритм Нагла - отправляем сразу
        socket.NoDelay = true;

        var timeout = ( int ) requestTimeout.TotalMilliseconds;
        socket.SendTimeout = timeout;
        socket.ReceiveTimeout = timeout;

        return socket;
    }

    public static TcpPeer Create(NodeId currentNodeId,
                                 NodeId nodeId,
                                 EndPoint endPoint,
                                 TimeSpan requestTimeout,
                                 ILogger logger)
    {
        var socket = CreateSocket(requestTimeout);
        var lazyStream = new Lazy<NetworkStream>(() => new NetworkStream(socket));

        return new TcpPeer(socket, endPoint, nodeId, currentNodeId, requestTimeout, lazyStream, logger);
    }
}