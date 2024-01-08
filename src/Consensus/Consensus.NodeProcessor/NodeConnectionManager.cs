using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Raft;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Models;

namespace Consensus.NodeProcessor;

public class NodeConnectionManager
{
    private readonly string _host;
    private readonly int _port;
    private readonly IRaftConsensusModule<Command, Response> _raft;
    private readonly TimeSpan _requestTimeout;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<NodeId, NodeConnectionProcessor> _nodes = new();

    public NodeConnectionManager(string host,
                                 int port,
                                 IRaftConsensusModule<Command, Response> raft,
                                 TimeSpan requestTimeout,
                                 ILogger logger)
    {
        _host = host;
        _port = port;
        _raft = raft;
        _requestTimeout = requestTimeout;
        _logger = logger;
    }

    public void Run(CancellationToken token)
    {
        _logger.Information("Модуль взаимодействия с другими узлами запускается");
        var endpoint = GetListenAddress();
        using var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            server.Bind(endpoint);
        }
        catch (Exception e)
        {
            _logger.Fatal(e, "Не удалось связать адрес {Endpoint} для прослушивания", endpoint.ToString());
            throw;
        }

        _logger.Information("Начинаю прослушивать входящие запросы");
        server.Listen();

        try
        {
            RunMainLoop(token, server);
        }
        catch (OperationCanceledException)
        {
            _logger.Information("Запрошено завершение работы. Закрываю все соединения");
        }
        finally
        {
            server.Shutdown(SocketShutdown.Receive);
            server.Close();
        }

        foreach (var node in _nodes)
        {
            node.Value.Dispose();
        }
    }

    private void RunMainLoop(CancellationToken token, Socket server)
    {
        while (token.IsCancellationRequested is false)
        {
            var client = server.Accept();
            _ = ProcessConnectedClientAsync(token, client);
        }
    }

    private async Task ProcessConnectedClientAsync(CancellationToken token, Socket client)
    {
        var clientAddress = client.RemoteEndPoint?.ToString();
        _logger.Debug("Подключился узел по адресу {Address}", clientAddress);
        try
        {
            var packetClient = new PacketClient(client);
            if (await TryAuthenticateAsync(packetClient, token) is { } nodeId)
            {
                try
                {
                    await packetClient.SendAsync(new ConnectResponsePacket(true), token);
                }
                catch (Exception e)
                {
                    _logger.Warning(e, "Ошибка во время установления соединения с узлом");
                    await client.DisconnectAsync(false, token);
                    client.Close();
                    client.Dispose();
                    return;
                }

                _logger.Information("Подключился узел {Id}. Начинаю обработку его запросов", nodeId.Id);
                BeginNewClientSession(nodeId, packetClient, token);
            }
            else
            {
                _logger.Debug(
                    "Узел по адресу {Address} не смог подключиться: не удалось получить Id хоста. Закрываю соединение",
                    clientAddress);
                await packetClient.SendAsync(new ConnectResponsePacket(false), token);
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

    private void BeginNewClientSession(NodeId id, PacketClient client, CancellationToken token)
    {
        var requestTimeoutMs = ( int ) _requestTimeout.TotalMilliseconds;
        client.Socket.SendTimeout = requestTimeoutMs;
        client.Socket.ReceiveTimeout = requestTimeoutMs;
        client.Socket.NoDelay = true;

        var processor = new NodeConnectionProcessor(id, client, _raft,
            _logger.ForContext("SourceContext", $"NodeConnectionProcessor({id.Id})"))
            {
                CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token)
            };
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

    private static async Task<NodeId?> TryAuthenticateAsync(PacketClient client, CancellationToken token)
    {
        var packet = await client.ReceiveAsync(token);
        if (packet is {PacketType: NodePacketType.ConnectRequest})
        {
            var request = ( ConnectRequestPacket ) packet;
            return request.Id;
        }

        return null;
    }

    private EndPoint GetListenAddress()
    {
        _logger.Debug("Хост: {Host}", _host);
        if (IPAddress.TryParse(_host, out var address))
        {
            return new IPEndPoint(address, _port);
        }

        var found = Dns.GetHostAddresses(_host)
                       .Where(a => a.AddressFamily == AddressFamily.InterNetwork)
                       .ToArray();

        switch (found)
        {
            case []:
                _logger.Fatal("Для переданного хоста {Host} не удалось найти IP адреса для биндинга", _host);
                throw new ApplicationException("Не удалось получить IP адрес для биндинга");
            case [var ip]:
                _logger.Information("По переданному хосту найден IP адрес {Address}", ip.ToString());
                return new IPEndPoint(ip, _port);
            default:
                _logger.Fatal(
                    "Для переданного хоста найдено более одного IP адреса. Необходимо указать только 1. {Addresses}",
                    found.Select(x => x.ToString()));
                throw new ApplicationException("Для переданного хоста найдено более 1 адреса");
        }
    }
}