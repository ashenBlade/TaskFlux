using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Raft.Core;
using Raft.Core.Node;
using Raft.Network;
using Raft.Network.Packets;
using Raft.Network.Socket;
using Raft.Server.Options;
using Serilog;

namespace Raft.Server;

public class ExternalConnectionManager
{
    private readonly string _host;
    private readonly int _port;
    private readonly RaftNode _raft;
    private readonly NetworkOptions _networkOptions;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<NodeId, NodeConnectionProcessor> _nodes = new();

    public ExternalConnectionManager(string host, int port, RaftNode raft, NetworkOptions networkOptions, ILogger logger)
    {
        _host = host;
        _port = port;
        _raft = raft;
        _networkOptions = networkOptions;
        _logger = logger;
    }

    private async ValueTask<IPAddress> GetListenAddress()
    {
        if (IPAddress.TryParse(_host, out var address))
        {
            return address;
        }

        var entry = await Dns.GetHostEntryAsync(_host);
        return entry.AddressList[0];
    }
    
    public async Task RunAsync(CancellationToken token)
    {
        _logger.Information("Начинаю принимать входящие сообщения");
        _logger.Debug("Разрешаю имя хоста для прослушивания");
        var address = await GetListenAddress();
        var listener = new TcpListener(address, _port);
        _logger.Debug("Начинаю прослушивать адрес {Address}:{Port}", _host, _port);
        listener.Start();
        try
        {
            while (token.IsCancellationRequested is false)
            {
                var client = await listener.AcceptTcpClientAsync(token);
                _logger.Debug("Клиент {Address} подключился. Начинаю обработку его запроса",
                    client.Client.RemoteEndPoint?.ToString());
                try
                {
                    var conn = new SocketNodeConnection(client.Client);
                    if (await TryAuthenticateAsync(conn) is {} nodeId)
                    {
                        var connection = new RemoteSocketNodeConnection(client.Client,
                            _logger.ForContext("SourceContext", $"NodeConnectionProcessor({nodeId.Value})"));
                        await connection.SendAsync(new ConnectResponsePacket(true), token);
                        BeginNewClientSession(nodeId, connection, client, token);
                    }
                    else
                    {
                        _logger.Debug("Клиент с адресом {Address} не смог подключиться: не удалось получить Id хоста. Закрываю соединение",
                            client.Client.RemoteEndPoint?.ToString());
                        await conn.SendAsync(new ConnectResponsePacket(false), token);
                        client.Close();
                        client.Dispose();
                    }
                }
                catch (Exception e)
                {
                    _logger.Warning(e, "Поймано необработанное исключение при подключении клиента {Address}", client.Client.RemoteEndPoint?.ToString());
                    client.Close();
                    client.Dispose();
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.Information("Запрошено завершение работы. Закрываю все соединения");    
        }
        finally
        {
            listener.Stop();
        }
        
        foreach (var node in _nodes)
        {
            node.Value.Dispose();
        }
    }

    private void BeginNewClientSession(NodeId id, IRemoteNodeConnection connection, TcpClient client, CancellationToken token)
    {
        var processor = new NodeConnectionProcessor(id, connection, client, _raft, 
            _logger.ForContext("SourceContext", $"ОбработчикКлиента{id.Value}"))
            {
                CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token)
            };
        _logger.Debug("Добавляю узел в список обрабатываемых");
        _nodes.AddOrUpdate(id,
            _ => processor,
            (_, old) =>
            {
                _logger.Verbose("В списке соединений уже было соединение с для текущего Id. Закрываю старое соединение");
                old.Dispose();
                return processor;
            });
        _logger.Debug("Начинаю обработку клиента");
        _ = processor.ProcessClientBackground();
    }

    private static async Task<NodeId?> TryAuthenticateAsync(INodeConnection connection)
    {
        var packet = await connection.ReceiveAsync();
        if (packet is {PacketType: PacketType.ConnectRequest})
        {
            var request = ( ConnectRequestPacket ) packet;
            return request.Id;
        }

        return null; 
    }
}