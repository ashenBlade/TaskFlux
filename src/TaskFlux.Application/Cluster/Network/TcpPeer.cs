using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using Serilog;
using TaskFlux.Application.Cluster.Network.Packets;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Cluster.Network.Exceptions;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Core;

namespace TaskFlux.Application.Cluster.Network;

public class TcpPeer : IPeer
{
    /// <summary>
    /// Эндпоинт узла, с которым мы общаемся
    /// </summary>
    private readonly EndPoint _endPoint;

    /// <summary>
    /// Id текущего узла.
    /// Нужен во время установления соединения
    /// </summary>
    private readonly NodeId _currentNodeId;

    /// <summary>
    /// Таймаут ожидания ответа от другого сервера
    /// </summary>
    private readonly TimeSpan _requestTimeout;

    /// <summary>
    /// Время ожидания между повторными запросами при потере соединения.
    /// Пока используется постоянное (одинаковое) время ожидания
    /// </summary>
    private readonly TimeSpan _connectionErrorDelay;

    private Socket _socket;
    private Lazy<NetworkStream> _lazy;
    private NetworkStream NetworkStream => _lazy.Value;

    private readonly ILogger _logger;
    public NodeId Id { get; }

    private TcpPeer(Socket socket,
                    EndPoint endPoint,
                    NodeId nodeId,
                    NodeId currentNodeId,
                    TimeSpan requestTimeout,
                    TimeSpan connectionErrorDelay,
                    Lazy<NetworkStream> lazy,
                    ILogger logger)
    {
        Id = nodeId;
        _socket = socket;
        _endPoint = endPoint;
        _currentNodeId = currentNodeId;
        _requestTimeout = requestTimeout;
        _connectionErrorDelay = connectionErrorDelay;
        _lazy = lazy;
        _logger = logger;
    }

    public AppendEntriesResponse SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        var response = SendPacketReconnectingCore(new AppendEntriesRequestPacket(request), token);

        switch (response.PacketType)
        {
            case NodePacketType.AppendEntriesResponse:
                return ( ( AppendEntriesResponsePacket ) response ).Response;

            case NodePacketType.AppendEntriesRequest:
            case NodePacketType.ConnectRequest:
            case NodePacketType.ConnectResponse:
            case NodePacketType.RequestVoteRequest:
            case NodePacketType.RequestVoteResponse:
            case NodePacketType.InstallSnapshotRequest:
            case NodePacketType.InstallSnapshotResponse:
            case NodePacketType.InstallSnapshotChunkRequest:
            case NodePacketType.InstallSnapshotChunkResponse:
            case NodePacketType.RetransmitRequest:
                _logger.Warning("От узла {Id} при отправке AppendEntries получен неожиданный пакет - {ResponseType}",
                    Id, response.PacketType);
                throw new UnexpectedPacketException(response, NodePacketType.AppendEntriesResponse);
        }

        return ThrowInvalidPacketType<AppendEntriesResponse>(response.PacketType);
    }

    private static T ThrowInvalidPacketType<T>(NodePacketType unknown)
    {
        throw new ArgumentOutOfRangeException(nameof(NodePacketType), unknown, "Неизвестный тип сетевого пакета");
    }

    /// <summary>
    /// Отправить пакет с учетом повторного переподключения при потере соединения
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный пакет или <c>null</c>, если соединение потеряно</returns>
    private NodePacket SendPacketReconnectingCore(NodePacket packet, CancellationToken token)
    {
        Debug.Assert(packet != null, "packet != null", "Отправляемый пакет не должен быть null");

        // Подобная проверка полезна в случаях:
        // - Это самый первый вызов (т.е. ранее не было подключений)
        // - Предыдущий вызов был отменен и подключение не было установлено
        if (!_socket.Connected)
        {
            EstablishConnection(token);
        }

        while (true)
        {
            // Предыдущая проверка может быть ложной
            // В таком случае, поймаем исключение и выполним повторное подключение
            try
            {
                return SendPacketReturning(packet, token);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
            catch (InvalidOperationException)
            {
            }

            WaitConnectionErrorDelay(token);
            UpdateSocketState();
            EstablishConnection(token);
        }
    }

    public RequestVoteResponse SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        _logger.Debug("Отправляю RequestVote запрос на узел {Id}", Id);
        var response = SendPacketReconnectingCore(new RequestVoteRequestPacket(request), token);

        switch (response.PacketType)
        {
            case NodePacketType.RequestVoteResponse:
                var requestVoteResponsePacket = ( RequestVoteResponsePacket ) response;
                return requestVoteResponsePacket.Response;

            case NodePacketType.AppendEntriesRequest:
            case NodePacketType.AppendEntriesResponse:
            case NodePacketType.ConnectRequest:
            case NodePacketType.ConnectResponse:
            case NodePacketType.RequestVoteRequest:
            case NodePacketType.InstallSnapshotRequest:
            case NodePacketType.InstallSnapshotResponse:
            case NodePacketType.InstallSnapshotChunkRequest:
            case NodePacketType.InstallSnapshotChunkResponse:
            case NodePacketType.RetransmitRequest:
                _logger.Warning(
                    "От узла {Id} получен неожиданный пакет при отправке RequestVote запроса - {ResponseType}", Id,
                    response.PacketType);
                throw new UnexpectedPacketException(response, NodePacketType.RequestVoteResponse);
        }

        return ThrowInvalidPacketType<RequestVoteResponse>(response.PacketType);
    }

    public InstallSnapshotResponse SendInstallSnapshot(InstallSnapshotRequest request,
                                                       CancellationToken token)
    {
        // Проверка через Connected плохая, но неплохо для начала.
        // Если соединение все же было разорвано, то заметим это при отправке заголовка

        if (!_socket.Connected)
        {
            EstablishConnection(token);
        }

        while (true)
        {
            try
            {
                return SendInstallSnapshotCore(request, token);
            }
            catch (IOException)
            {
            }
            catch (SocketException)
            {
            }

            _logger.Debug("Ошибка отправки снапшота. Делаю повторную попытку через {Delay}", _connectionErrorDelay);

            // Если соединение разорвано, то необходимо полностью переподключиться
            WaitConnectionErrorDelay(token);
            UpdateSocketState();
            EstablishConnection(token);
        }
    }

    /// <summary>
    /// Основной метод для отправки снапшота на другой узел.
    /// Обрабатывает только удачный путь (happy path).
    /// В случае разрыва сети кидается соответствующее исключение.
    /// </summary>
    /// <param name="request">Запрос, который нужно отправить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Конечный ответ сервера</returns>
    private InstallSnapshotResponse SendInstallSnapshotCore(InstallSnapshotRequest request,
                                                            CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        _logger.Information("Отправляю снапшот на узел {NodeId}", Id);

        // 1. Отправляем заголовок

        // Попытку повторного установления соединения делаем только один раз - при отправке заголовка.
        // Если возникла ошибка, то полностью снапшот отправим в другом запросе
        _logger.Debug("Отправляю InstallSnapshotChunk пакет");
        var headerResponse =
            SendPacketReconnectingCore(
                new InstallSnapshotRequestPacket(request.Term, request.LeaderId, request.LastEntry), token);

        if (TryGetInstallSnapshotResponse(headerResponse, out var installSnapshotResponse))
        {
            return installSnapshotResponse;
        }

        // 2. Поочередно отправляем чанки снапшота
        var chunkNumber = 1;
        foreach (var chunk in request.Snapshot.GetAllChunks(token))
        {
            token.ThrowIfCancellationRequested();
            _logger.Debug("Отправляю {Number} чанк данных", chunkNumber);

            // Отправка без переподключения, т.к. если сделать, то узел начнет принимать чанки где-то с середины и вообще не будет знать что происходит
            var chunkResponse = SendPacketReturning(new InstallSnapshotChunkRequestPacket(chunk), token);
            if (TryGetInstallSnapshotResponse(chunkResponse, out installSnapshotResponse))
            {
                return installSnapshotResponse;
            }

            chunkNumber++;
        }

        _logger.Debug("Отправка чанков закончена. Отправляю последний пакет");
        var lastChunkResponse = SendPacketReturning(new InstallSnapshotChunkRequestPacket(Memory<byte>.Empty), token);

        if (TryGetInstallSnapshotResponse(lastChunkResponse, out installSnapshotResponse))
        {
            return installSnapshotResponse;
        }

        throw new UnexpectedPacketException(lastChunkResponse, NodePacketType.InstallSnapshotResponse);

        static bool TryGetInstallSnapshotResponse(NodePacket packet, out InstallSnapshotResponse response)
        {
            if (packet.PacketType is NodePacketType.InstallSnapshotResponse)
            {
                response = new InstallSnapshotResponse(( ( InstallSnapshotResponsePacket ) packet ).CurrentTerm);
                return true;
            }

            if (packet.PacketType is not NodePacketType.InstallSnapshotChunkResponse)
            {
                throw new UnexpectedPacketException(packet, NodePacketType.InstallSnapshotChunkResponse);
            }

            response = default!;
            return false;
        }
    }


    /// <summary>
    /// Метод для однократной отправки пакета с учетом таймаутов и RetransmitRequest'ом.
    /// Если соединение обрывается, то будет выкинуто соответствующее исключение.
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный пакет, или <c>null</c> если соединение потеряно</returns>
    /// <exception cref="SocketException">Ошибка при отправке/приеме пакета</exception>
    /// <exception cref="IOException">Ошибка при отправке/приеме пакета</exception>
    private NodePacket SendPacketReturning(NodePacket packet, CancellationToken token)
    {
        Debug.Assert(packet is not null, "packet is not null", "Отправляемый пакет не должен быть null");

        while (true)
        {
            token.ThrowIfCancellationRequested();
            var stream = NetworkStream;
            try
            {
                packet.Serialize(stream);
                stream.Flush();
                token.ThrowIfCancellationRequested();
                var response = NodePacket.Deserialize(stream);
                if (response.PacketType is NodePacketType.RetransmitRequest)
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
                _logger.Warning("Таймаут ожидания при отправке пакета {PacketType} превышен. Делаю повторную отправку",
                    packet.PacketType);
            }
        }
    }

    /// <summary>
    /// Метод для установки НОВОГО соединения.
    /// Должен вызываться либо:
    /// 1. Когда был создан новый сокет и никаких обращений еще не было, либо
    /// 2. Когда старое соединение было разорвано
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="AuthenticationException">Возникла ошибка при авторизации на другом узле</exception>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    private void EstablishConnection(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();

        while (true)
        {
            // 1. Подключаемся к узлу - простой Connect на сокете
            while (!SocketConnect())
            {
                _logger.Debug("Не удалось подключиться к узлу. Делаю повторную попытку через {Timeout}",
                    _connectionErrorDelay);
                WaitConnectionErrorDelay(token);
            }

            // 2. Отправляем пакет к уже подключенному узлу
            if (Authorize(token))
            {
                /*
                 * Если попали сюда, то значит авторизация прошла успешно.
                 * В случае ошибки будет выкинуто исключение, т.к. эту ситуацию мы обработать не можем
                 */
                return;
            }

            _logger.Debug("Соединение потеряно в момент авторизации. Делаю повторную попытку через {Timeout}",
                _connectionErrorDelay);
            WaitConnectionErrorDelay(token);

            // Состояние сокета обновляем, т.к. повторное подключение закрытым сокетом выполнять нельзя
            UpdateSocketState();
        }

        bool SocketConnect()
        {
            try
            {
                _logger.Information("Подключаюсь к узлу {NodeId}", Id);
                _socket.Connect(_endPoint);
                return true;
            }
            catch (SocketException)
            {
                return false;
            }
            catch (IOException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                /*
                 * Такое иногда может случиться, если использовать закрытый сокет
                 */
                UpdateSocketState();
                return false;
            }
        }

        bool Authorize(CancellationToken ct)
        {
            _logger.Information("Авторизуюсь на узле {NodeId}", Id);
            try
            {
                var response = SendPacketReturning(new ConnectRequestPacket(_currentNodeId), ct);
                switch (response.PacketType)
                {
                    case NodePacketType.ConnectResponse:

                        var connectResponsePacket = ( ConnectResponsePacket ) response;
                        if (connectResponsePacket.Success)
                        {
                            _logger.Information("Авторизация прошла успешно");
                            return true;
                        }

                        throw new AuthenticationException("Ошибка при попытке авторизации на узле");

                    case NodePacketType.ConnectRequest:
                    case NodePacketType.RequestVoteRequest:
                    case NodePacketType.RequestVoteResponse:
                    case NodePacketType.AppendEntriesRequest:
                    case NodePacketType.AppendEntriesResponse:
                    case NodePacketType.InstallSnapshotRequest:
                    case NodePacketType.InstallSnapshotChunkRequest:
                    case NodePacketType.InstallSnapshotResponse:
                    case NodePacketType.RetransmitRequest:
                        _logger.Warning(
                            "От узла {NodeId} получен неожиданный пакет: {PacketType}. Ожидался: {ExpectedPacketType}",
                            Id, response.PacketType, NodePacketType.ConnectResponse);
                        throw new UnexpectedPacketException(response, NodePacketType.ConnectResponse);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(response.PacketType), response.PacketType,
                            "Неизвестный маркер типа пакета");
                }
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
            catch (InvalidOperationException)
            {
            }

            return false;
        }
    }

    /// <summary>
    /// Метод для замены старого сокета и NetworkStream, когда соединение было разорвано
    /// </summary>
    private void UpdateSocketState()
    {
        var newSocket = CreateSocket(_requestTimeout);
        var newLazy = new Lazy<NetworkStream>(() => new NetworkStream(newSocket));
        _socket.Close();
        _socket = newSocket;
        _lazy = newLazy;
    }

    private static Socket CreateSocket(TimeSpan requestTimeout)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        // Убираем алгоритм Нагла - отправляем сразу
        socket.NoDelay = true;

        // Таймауты на чтение и отправку должны выставляться, т.к. 
        // если соединение внезапно разорвется и таймаут не будет выставлен,
        // то можем встрять в вечном ожидании - если таймаут произошел, то просто заново отправим пакет
        var timeout = ( int ) requestTimeout.TotalMilliseconds;
        socket.SendTimeout = timeout;
        socket.ReceiveTimeout = timeout;

        return socket;
    }

    private void WaitConnectionErrorDelay(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        using var _ = token.UnsafeRegister(static o => ( ( Thread ) o! ).Interrupt(), Thread.CurrentThread);
        try
        {
            Thread.Sleep(_connectionErrorDelay);
        }
        catch (ThreadInterruptedException)
        {
        }

        token.ThrowIfCancellationRequested();
    }

    public static TcpPeer Create(NodeId currentNodeId,
                                 NodeId nodeId,
                                 EndPoint endPoint,
                                 TimeSpan requestTimeout,
                                 TimeSpan connectionErrorDelay,
                                 ILogger logger)
    {
        var socket = CreateSocket(requestTimeout);
        var lazyStream = new Lazy<NetworkStream>(() => new NetworkStream(socket));

        return new TcpPeer(socket, endPoint, nodeId, currentNodeId, requestTimeout, connectionErrorDelay, lazyStream,
            logger);
    }
}