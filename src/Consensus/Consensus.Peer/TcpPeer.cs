using System.ComponentModel;
using System.Diagnostics;
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

// TODO: обновить отправку снапшота и добавить логику переподключения вечного (м.б. с таймаутами)
public class TcpPeer : IPeer
{
    private static BinaryPacketDeserializer Deserializer => BinaryPacketDeserializer.Instance;

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
            case RaftPacketType.AppendEntriesResponse:
                var appendEntriesResponsePacket = ( AppendEntriesResponsePacket ) response;
                return appendEntriesResponsePacket.Response;

            case RaftPacketType.AppendEntriesRequest:
            case RaftPacketType.ConnectRequest:
            case RaftPacketType.ConnectResponse:
            case RaftPacketType.RequestVoteRequest:
            case RaftPacketType.RequestVoteResponse:
            case RaftPacketType.InstallSnapshotRequest:
            case RaftPacketType.InstallSnapshotResponse:
            case RaftPacketType.InstallSnapshotChunkRequest:
            case RaftPacketType.InstallSnapshotChunkResponse:
            case RaftPacketType.RetransmitRequest:
                throw new UnexpectedPacketException(response, RaftPacketType.AppendEntriesResponse);
        }

        throw new InvalidEnumArgumentException(nameof(response.PacketType),
            ( int ) response.PacketType,
            typeof(RaftPacketType));
    }

    /// <summary>
    /// Отправить пакет с учетом повторного переподключения при потере соединения
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный пакет или <c>null</c>, если соедение потеряно</returns>
    private RaftPacket SendPacketReconnectingCore(RaftPacket packet, CancellationToken token)
    {
        Debug.Assert(packet != null, "packet != null", "Отправляемый пакет не должен быть null");

        while (true)
        {
            while (!_socket.Connected)
            {
                _logger.Information("Устанавливаю соединение с узлом");
                EstablishConnection(token);
            }

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

            token.ThrowIfCancellationRequested();
            Thread.Sleep(_connectionErrorDelay);
            UpdateSocketState();
        }
    }


    public RequestVoteResponse SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        var response = SendPacketReconnectingCore(new RequestVoteRequestPacket(request), token);

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
            case RaftPacketType.InstallSnapshotResponse:
            case RaftPacketType.InstallSnapshotChunkRequest:
            case RaftPacketType.InstallSnapshotChunkResponse:
            case RaftPacketType.RetransmitRequest:
                throw new UnexpectedPacketException(response, RaftPacketType.RequestVoteResponse);
        }

        throw new InvalidEnumArgumentException(nameof(response.PacketType), ( int ) response.PacketType,
            typeof(RaftPacketType));
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

            var waitDelay = ( int ) _connectionErrorDelay.TotalMilliseconds;
            _logger.Information("Ошибка отправки снапшота. Делаю повторную попытку через {Delay}мс", waitDelay);
            Thread.Sleep(waitDelay);
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
        var headerPacket = new InstallSnapshotRequestPacket(request.Term, request.LeaderId, request.LastEntry);
        _logger.Debug("Отправляю InstallSnapshotChunk пакет");

        // Попытку повторного установления соединения делаем только один раз - при отправке заголовка.
        // Если возникла ошибка, то полностью снапшот отправим в другом запросе
        // TODO: заменить своим
        var headerResponse = SendPacketReconnectingCore(headerPacket, token);

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

            var chunkResponse = SendPacketReturning(new InstallSnapshotChunkRequestPacket(chunk), token);
            if (TryGetInstallSnapshotResponse(chunkResponse, out installSnapshotResponse))
            {
                return installSnapshotResponse;
            }
        }

        _logger.Debug("Отправка чанков закончена. Отправляю последний пакет");
        var lastChunkResponse = SendPacketReturning(new InstallSnapshotChunkRequestPacket(Memory<byte>.Empty), token);

        if (TryGetInstallSnapshotResponse(lastChunkResponse, out installSnapshotResponse))
        {
            return installSnapshotResponse;
        }

        throw new UnexpectedPacketException(lastChunkResponse, RaftPacketType.InstallSnapshotResponse);

        static bool TryGetInstallSnapshotResponse(RaftPacket packet, out InstallSnapshotResponse response)
        {
            if (packet.PacketType is RaftPacketType.InstallSnapshotResponse)
            {
                response = new InstallSnapshotResponse(( ( InstallSnapshotResponsePacket ) packet ).CurrentTerm);
                return true;
            }

            if (packet.PacketType is not RaftPacketType.InstallSnapshotChunkResponse)
            {
                throw new UnexpectedPacketException(packet, RaftPacketType.InstallSnapshotChunkResponse);
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
    private RaftPacket SendPacketReturning(RaftPacket packet, CancellationToken token)
    {
        Debug.Assert(packet is not null, "packet is not null", "Отправляемый пакет не должен быть null");
        token.ThrowIfCancellationRequested();

        while (true)
        {
            try
            {
                packet.Serialize(NetworkStream);
                token.ThrowIfCancellationRequested();

                var response = Deserializer.Deserialize(NetworkStream);
                if (response.PacketType is RaftPacketType.RetransmitRequest)
                {
                    // Повторно отправляем запрос
                    token.ThrowIfCancellationRequested();
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
        _logger.Debug("Начинаю устанавливать соединение");

        while (!ConnectCore())
        {
            token.ThrowIfCancellationRequested();
            Thread.Sleep(_connectionErrorDelay);
            UpdateSocketState();
        }

        return;

        // Основной метод для подключения
        bool ConnectCore()
        {
            try
            {
                _socket.Connect(_endPoint);

                _logger.Debug("Делаю запрос авторизации");
                var response = SendPacketReturning(new ConnectRequestPacket(_currentNodeId), token);

                switch (response.PacketType)
                {
                    case RaftPacketType.ConnectResponse:

                        var connectResponsePacket = ( ConnectResponsePacket ) response;
                        if (connectResponsePacket.Success)
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
                    case RaftPacketType.InstallSnapshotChunkRequest:
                    case RaftPacketType.InstallSnapshotResponse:
                    case RaftPacketType.RetransmitRequest:
                        throw new UnexpectedPacketException(response, RaftPacketType.ConnectResponse);

                    default:
                        throw new InvalidEnumArgumentException(nameof(response.PacketType), ( int ) response.PacketType,
                            typeof(RaftPacketType));
                }
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
                return false;
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