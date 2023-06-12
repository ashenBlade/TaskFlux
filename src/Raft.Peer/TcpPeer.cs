using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
using Serilog;

namespace Raft.Peer;

public class TcpPeer: IPeer, IDisposable
{
    private readonly ISocket _client;
    private readonly string _host;
    private readonly int _port;
    private readonly ILogger _logger;
    
    internal TcpPeer(PeerId id, ISocket client, string host, int port)
    {
        _client = client;
        _host = host;
        _port = port;
        Id = id;
        _logger = Log.ForContext<TcpPeer>();
    }

    public PeerId Id { get; }
    
    public async Task<HeartbeatResponse?> SendHeartbeat(HeartbeatRequest request, CancellationToken token)
    {
        var data = Helpers.Serialize(request);
        try
        {
            _logger.Verbose("Делаю запрос Heartbeat на узел {PeerId}", Id);
            await _client.SendAsync(data, token);
            
            int read;
            var memory = new MemoryStream();
            var responseBuffer = new byte[128];
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            while ((read = await _client.ReadAsync(responseBuffer, token)) > 0)
            {
                memory.Write(responseBuffer, 0, read);
                if (read < responseBuffer.Length)
                {
                    break;
                }
            }
            
            _logger.Verbose("Ответ от узла {PeerId} получен. Десериализую", Id);
            return Helpers.DeserializeHeartbeatResponse(memory.ToArray());
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
        try
        {
            _logger.Verbose("Делаю запрос RequestVote на узел {PeerId}", Id);
            await _client.SendAsync(data, token);
            
            int read;
            var memoryStream = new MemoryStream();
            var responseBuffer = new byte[128];
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            while ((read = await _client.ReadAsync(responseBuffer, token)) > 0)
            {
                memoryStream.Write(responseBuffer, 0, read);
                if (read < responseBuffer.Length)
                {
                    break;
                }
            }
            
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

    public static TcpPeer Create(PeerId id, PeerId currentNodeId, string host, int port)
    {
        ArgumentNullException.ThrowIfNull(host);
        if (port < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(port), port, "Порт не может быть отрицательным");
        }

        var socket = new RaftTcpSocket(host, port, currentNodeId, Log.ForContext<RaftTcpSocket>());
        var peer = new TcpPeer(id, socket, host, port);
        return peer;
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}