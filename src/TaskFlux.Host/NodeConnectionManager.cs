using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Consensus.Core;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Peer;
using Serilog;
using TaskFlux.Requests;

namespace TaskFlux.Host;

public class NodeConnectionManager
{
    private readonly string _host;
    private readonly int _port;
    private readonly RaftConsensusModule<IRequest, IResponse> _raft;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<NodeId, NodeConnectionProcessor> _nodes = new();

    public NodeConnectionManager(string host, int port, RaftConsensusModule<IRequest, IResponse> raft, ILogger logger)
    {
        _host = host;
        _port = port;
        _raft = raft;
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
            var clientAddress = client.RemoteEndPoint?.ToString();
            _logger.Debug("Клиент {Address} подключился. Начинаю обработку его запроса", clientAddress);
            _ = ProcessConnectedClientAsync(token, client, clientAddress);
        }
    }

    private async Task ProcessConnectedClientAsync(CancellationToken token, Socket client, string? clientAddress)
    {
        try
        {
            var packetClient = new PacketClient(client);
            if (await TryAuthenticateAsync(packetClient, token) is { } nodeId)
            {
                var success = await packetClient.SendAsync(new ConnectResponsePacket(true), token);
                if (!success)
                {
                    _logger.Information("Узел {Node} отключился во время ответа на пакет успешной авторизации", nodeId);
                    await client.DisconnectAsync(false, token);
                    client.Close();
                    client.Dispose();
                    return;
                }

                BeginNewClientSession(nodeId, packetClient, token);
            }
            else
            {
                _logger.Debug(
                    "Клиент с адресом {Address} не смог подключиться: не удалось получить Id хоста. Закрываю соединение",
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
        var processor = new NodeConnectionProcessor(id, client, _raft, 
            _logger.ForContext("SourceContext", $"NodeConnectionProcessor({id.Value})"))
            {
                CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token)
            };
        _nodes.AddOrUpdate(id,
            _ => processor,
            (_, old) =>
            {
                _logger.Information("В списке соединений уже было соединение с для текущего Id. Закрываю старое соединение");
                old.Dispose();
                return processor;
            });
        _logger.Debug("Начинаю обработку клиента");
        _ = processor.ProcessClientBackground();
    }

    private static async Task<NodeId?> TryAuthenticateAsync(PacketClient client, CancellationToken token)
    {
        var packet = await client.ReceiveAsync(token);
        if (packet is {PacketType: PacketType.ConnectRequest})
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
                _logger.Fatal("Для переданного хоста найдено более одного IP адреса. Необходимо указать только 1. {Addresses}", found.Select(x => x.ToString()));
                throw new ApplicationException("Для переданного хоста найдено более 1 адреса");
        }
    }
}