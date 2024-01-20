using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Serilog;
using TaskFlux.Consensus.Cluster.Network;
using TaskFlux.Consensus.Cluster.Network.Packets;
using TaskFlux.Core;
using TaskFlux.Core.Commands;

namespace TaskFlux.Consensus.Cluster;

public class NodeConnectionManager : IDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly RaftConsensusModule<Command, Response> _module;
    private readonly TimeSpan _requestTimeout;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<NodeId, NodeConnectionProcessor> _nodes = new();
    private readonly (Thread WorkerThread, Socket Server, CancellationTokenSource Lifetime)? _workerData;

    public NodeConnectionManager(string host,
                                 int port,
                                 RaftConsensusModule<Command, Response> module,
                                 TimeSpan requestTimeout,
                                 ILogger logger)
    {
        _host = host;
        _port = port;
        _module = module;
        _requestTimeout = requestTimeout;
        _logger = logger;
        _workerData = ( new Thread(ThreadWorker), CreateServerSocket(), new CancellationTokenSource() );
    }

    private Socket CreateServerSocket()
    {
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        var endpoint = GetListenAddress();
        _logger.Debug("Прослушивание подключений узлов кластера по адресу: {EndPoint}", endpoint);
        try
        {
            server.Bind(endpoint);
        }
        catch (Exception e)
        {
            _logger.Fatal(e, "Не удалось связать адрес {Endpoint} для прослушивания", endpoint.ToString());
            throw;
        }

        return server;
    }

    private void ThreadWorker()
    {
        Debug.Assert(_workerData is not null, "_workerData is not null",
            "Данные для работы сервера должны быть инициализированы");
        var (_, server, cts) = _workerData.Value;

        _logger.Information(
            "Модуль обработки запросов узлов кластера запускается. Начинаю прослушивать входящие запросы узлов");
        server.Listen(_module.PeerGroup.Peers.Count + 1);
        try
        {
            while (true)
            {
                var client = server.Accept();
                _ = ProcessConnectedClientAsync(client, cts);
            }
        }
        catch (SocketException se) when (se.SocketErrorCode is SocketError.Interrupted)
        {
        }
    }

    public void Start()
    {
        if (_workerData is var (thread, _, _))
        {
            thread.Start();
        }
        else
        {
            throw new InvalidOperationException("Обработчик запросов узлов кластера завершил свою работу");
        }
    }

    private async Task ProcessConnectedClientAsync(Socket client, CancellationTokenSource cts)
    {
        await Task.Yield();

        var clientAddress = client.RemoteEndPoint?.ToString();
        _logger.Debug("Подключился узел по адресу {Address}", clientAddress);
        try
        {
            var packetClient = new PacketClient(client);
            if (await TryAuthenticateAsync(packetClient) is { } nodeId)
            {
                try
                {
                    await packetClient.SendAsync(new ConnectResponsePacket(true), CancellationToken.None);
                }
                catch (Exception e)
                {
                    _logger.Warning(e, "Ошибка во время установления соединения с узлом");
                    await client.DisconnectAsync(false);
                    client.Close();
                    client.Dispose();
                    return;
                }

                _logger.Information("Подключился узел {Id}. Начинаю обработку его запросов", nodeId.Id);
                BeginNewClientSession(nodeId, cts, packetClient);
            }
            else
            {
                _logger.Debug(
                    "Узел по адресу {Address} не смог подключиться: не удалось получить Id хоста. Закрываю соединение",
                    clientAddress);
                await packetClient.SendAsync(new ConnectResponsePacket(false), CancellationToken.None);
                client.Close();
                client.Dispose();
            }
        }
        catch (Exception e)
        {
            _logger.Warning(e, "Поймано необработанное исключение при подключении клиента {Address}", clientAddress);
            client.Close();
            client.Dispose();
        }
    }

    private void BeginNewClientSession(NodeId id, CancellationTokenSource cts, PacketClient client)
    {
        var requestTimeoutMs = ( int ) _requestTimeout.TotalMilliseconds;
        client.Socket.SendTimeout = requestTimeoutMs;
        client.Socket.ReceiveTimeout = requestTimeoutMs;
        client.Socket.NoDelay = true;

        var processor = new NodeConnectionProcessor(id, client, _module,
            _logger.ForContext("SourceContext", $"NodeConnectionProcessor({id.Id})")) {CancellationTokenSource = cts};
        _nodes.AddOrUpdate(id,
            static (_, p) => p.Processor,
            static (_, old, arg) =>
            {
                arg.Logger.Information(
                    "В списке соединений уже было соединение с для текущего Id. Закрываю старое соединение");
                old.Dispose();
                return arg.Processor;
            },
            ( Processor: processor, Logger: _logger ));
        _logger.Debug("Начинаю обработку клиента");
        _ = processor.ProcessClientBackground();
    }

    private static async Task<NodeId?> TryAuthenticateAsync(PacketClient client)
    {
        var packet = await client.ReceiveAsync(CancellationToken.None);
        if (packet is {PacketType: NodePacketType.ConnectRequest})
        {
            var request = ( ConnectRequestPacket ) packet;
            return request.Id;
        }

        return null;
    }

    private IPEndPoint GetListenAddress()
    {
        // Для прослушивания сокет поддерживает только IP адрес
        if (IPAddress.TryParse(_host, out var address))
        {
            return new IPEndPoint(address, _port);
        }

        _logger.Verbose("Нахожу IP адреса для хоста {Host}", _host);
        var found = Dns.GetHostAddresses(_host)
                       .Where(a => a.AddressFamily == AddressFamily.InterNetwork)
                       .ToArray();
        if (found.Length == 1)
        {
            var ip = found[0];
            _logger.Debug("По адресу хоста {Host} найден IP адрес {IPAddress}", _host, ip);
            return new IPEndPoint(ip, _port);
        }

        if (found.Length == 0)
        {
            throw new ArgumentException($"Не удалось получить IP адрес для переданного хоста {_host}");
        }

        throw new ArgumentException(
            $"По переданному хосту найдено больше 1 IP адреса. Для работы требуется только 1 адрес. Найденные адреса: {string.Join(',', found.Select(a => a.ToString()))}");
    }

    public void Stop()
    {
        if (_workerData is var (_, server, cts))
        {
            // Останавливаем обработчиков узлов кластера
            cts.Cancel();

            // Прекращаем слушать сокет
            server.Shutdown(SocketShutdown.Both);
        }
    }

    public void Dispose()
    {
        if (_workerData is var (thread, server, cts))
        {
            cts.Dispose();
            server.Dispose();
            thread.Join();
        }
    }
}