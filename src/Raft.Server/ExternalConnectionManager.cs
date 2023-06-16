using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Raft.Core;
using Raft.Core.StateMachine;
using Raft.Peer;
using Serilog;

namespace Raft.Server;

public class ExternalConnectionManager
{
    private readonly string _host;
    private readonly int _port;
    private readonly RaftStateMachine _raft;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<PeerId, NodeConnectionProcessor> _nodes = new();

    public ExternalConnectionManager(string host, int port, RaftStateMachine raft, ILogger logger)
    {
        _host = host;
        _port = port;
        _raft = raft;
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
                    ( ( IPEndPoint ) client.Client.RemoteEndPoint! ).Address.ToString());
                try
                {
                    if (TryGetPeerId(client, out var peerId))
                    {
                        await ResponseOk(client, token);
                        BeginNewClientSession(peerId, client, token);
                    }
                    else
                    {
                        _logger.Debug("Клиент с адресом {@Address} не смог подключиться. Не удалось получить Id хоста",
                            client.Client.RemoteEndPoint);
                    }
                }
                catch (Exception e)
                {
                    _logger.Warning(e, "Ошибка при подключении клиента {@Address}", client.Client.RemoteEndPoint);
                    client.Dispose();
                }
            }
        }
        catch (OperationCanceledException canceledException)
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

    private void BeginNewClientSession(PeerId id, TcpClient client, CancellationToken token)
    {
        var processor = new NodeConnectionProcessor(id, client, _raft,
            _logger.ForContext("SourceContext", $"ОбработчикКлиента{id.Value}"))
            {
                CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token)
            };
        _logger.Debug("Добавляю ноду в список обрабатываемых");
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

    private async Task ResponseOk(TcpClient client, CancellationToken token)
    {
        var memory = new MemoryStream();
        var writer = new BinaryWriter(memory, Encoding.Default, true);
        writer.Write((byte)RequestType.Connect);
        writer.Write(true);
        var stream = client.GetStream();
        _logger.Debug("Отвечаю клиенту положительно");
        await stream.WriteAsync(memory.ToArray(), token);
    }

    private bool TryGetPeerId(TcpClient client, out PeerId id)
    {
        var stream = client.GetStream();
        var reader = new BinaryReader(stream, Encoding.UTF8, true);
        var marker = reader.ReadByte();
        if (marker is not (byte)RequestType.Connect)
        {
            _logger.Warning("От клиента {@Address} поступил запрос на присодинение, но первый пакет не был пакетом соединения", client.Client.RemoteEndPoint);
            id = PeerId.None;
            return false;
        }
        id = new( reader.ReadInt32() );
        _logger.Verbose("Подключенный клиент передал Id {Id}", id.Value);
        return true;
    }
}