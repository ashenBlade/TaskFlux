using System.Net;
using System.Net.Sockets;
using System.Text;
using Raft.Core;
using Raft.Peer;
using Serilog;

namespace Raft.Server;

public class ExternalConnectionsManager
{
    private readonly string _host;
    private readonly int _port;
    private readonly ILogger _logger;
    
    public ExternalConnectionsManager(string host, int port, ILogger logger)
    {
        _host = host;
        _port = port;
        _logger = logger;
    }

    public event EventHandler<(PeerId Id, TcpClient Client)>? PeerConnected;

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
        _logger.Debug("Запускаю прослушивателя");
        listener.Start();
        try
        {
            while (token.IsCancellationRequested is false)
            {
                var client = await listener.AcceptTcpClientAsync(token);
                _logger.Debug("Клиент {Address} подключился. Начинаю обработку его запроса", ( ( IPEndPoint ) client.Client.RemoteEndPoint! ).Address.ToString());
                try
                {
                    if (TryGetPeerId(client, out var peerId))
                    {
                        await ResponseOk(client, token);
                        OnPeerConnected((peerId, client));
                    }
                    else
                    {
                        _logger.Debug("Клиент с адресом {@Address} не смог подключиться. Не удалось получить Id хоста", client.Client.RemoteEndPoint);
                    }
                }
                catch (Exception e)
                {
                    _logger.Warning(e, "Ошибка при подключении клиента {@Address}", client.Client.RemoteEndPoint);
                    client.Dispose();
                }
            }
        }
        finally
        {
            listener.Stop();
        }
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

    private void OnPeerConnected((PeerId Id, TcpClient Client) e)
    {
        PeerConnected?.Invoke(this, e);
    }
}