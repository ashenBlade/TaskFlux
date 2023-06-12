using System.Net.Sockets;
using System.Text.Json;
using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
using Serilog;

namespace Raft.Peer;

public class TcpPeer: IPeer, IDisposable
{
    
    private readonly TcpClient _writer;
    private readonly string _host;
    private readonly int _port;
    private readonly object _communicationLock = new();
    private readonly ILogger _logger; 
    
    internal TcpPeer(PeerId id, TcpClient writer, string host, int port)
    {
        _writer = writer;
        _host = host;
        _port = port;
        Id = id;
        _logger = Log.ForContext<TcpPeer>();
    }

    public PeerId Id { get; }
    
    public async Task<HeartbeatResponse?> SendHeartbeat(HeartbeatRequest request, CancellationToken token)
    {
        var data = Helpers.Serialize(request);
        var stream = _writer.GetStream();
        try
        {
            _logger.Verbose("Делаю запрос Heartbeat на узел {PeerId}", Id);
            await stream.WriteAsync(data, token);
            
            int read;
            var memoryStream = new MemoryStream();
            var responseBuffer = new byte[1024];
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            do
            {
                read = await stream.ReadAsync(responseBuffer, token);
                memoryStream.Write(responseBuffer, 0, read);
            } while (read > 0);
            _logger.Verbose("Ответ от узла {PeerId} получен. Десериализую", Id);
            return Helpers.DeserializeHeartbeatResponse(memoryStream.ToArray());
        }
        catch (IOException io)
        {
            _logger.Debug(io, "Узел {PeerId} не ответил - проблемы с сетью", Id);
            return null;
        }
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        var data = Helpers.Serialize(request);
        var stream = _writer.GetStream();
        try
        {
            _logger.Verbose("Делаю запрос Heartbeat на узел {PeerId}", Id);
            await stream.WriteAsync(data, token);
            
            int read;
            var memoryStream = new MemoryStream();
            var responseBuffer = new byte[1024];
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            do
            {
                read = await stream.ReadAsync(responseBuffer, token);
                memoryStream.Write(responseBuffer, 0, read);
            } while (read > 0);
            _logger.Verbose("Ответ от узла {PeerId} получен. Десериализую", Id);
            try
            {
                return Helpers.DeserializeRequestVoteResponse(memoryStream.ToArray());
            }
            catch (ArgumentException argumentException)
            {
                _logger.Warning(argumentException, "Ошибка десериализации ответа от узла {PeerId}", Id);
                return null;
            }
        }
        catch (IOException io)
        {
            _logger.Debug(io, "Узел {PeerId} не ответил - проблемы с сетью", Id);
            return null;
        }
    }

    public Task SendAppendEntries(CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public static async Task<TcpPeer> ConnectAsync(PeerId id, string host, int port)
    {
        ArgumentNullException.ThrowIfNull(host);
        if (port < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(port), port, "Порт не может быть отрицательным");
        }

        var client = new TcpClient(host, port);

        await client.ConnectAsync(host, port);

        return new TcpPeer(id, client, host, port);
    }

    public void Dispose()
    {
        _writer.Dispose();
    }
}