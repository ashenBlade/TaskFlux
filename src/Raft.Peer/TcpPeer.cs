using System.Net.Sockets;
using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
using Raft.Peer.Exceptions;
using Serilog;
using Serilog.Context;

namespace Raft.Peer;

public class TcpPeer: IPeer, IDisposable
{
    private readonly ISocket _client;
    private readonly ILogger _logger;
    
    public TcpPeer(PeerId id, ISocket client, ILogger logger)
    {
        _client = client;
        Id = id;
        _logger = logger;
    }

    public PeerId Id { get; }
    
    public async Task<HeartbeatResponse?> SendHeartbeat(HeartbeatRequest request, CancellationToken token)
    {
        var data = Helpers.Serialize(request);
        try
        {
            _logger.Verbose("Делаю запрос Heartbeat на узел {PeerId}", Id);
            await _client.SendAsync(data, token);

            var memory = new MemoryStream();
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            await _client.ReadAsync(memory, token);

            _logger.Verbose("Ответ от узла {PeerId} получен. Десериализую", Id);
            return Helpers.DeserializeHeartbeatResponse(memory.ToArray());
        }
        catch (NetworkException network)
        {
            _logger.Debug(network, "Ошибка сети во время отправки Heartbeat");
            return null;
        }
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        byte[] response;
        var data = Helpers.Serialize(request);
        try
        {
            _logger.Verbose("Делаю запрос RequestVote на узел {PeerId}", Id);
            await _client.SendAsync(data, token);

            using var memoryStream = new MemoryStream();
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            await _client.ReadAsync(memoryStream, token);

            response = memoryStream.ToArray();
        }
        catch (NetworkException networkException)
        {
            _logger.Debug(networkException, "Во время отправки данных по сокету произошла ошибка сети");
            return null;
        }
        catch (SocketException socket)
        {
            _logger.Warning(socket, "Неизвестная ошибка сокета во время отправки данных к серерву");
            return null;
        }
        
        _logger.Verbose("Ответ от узла {PeerId} получен. Десериализую", Id);
        return Helpers.DeserializeRequestVoteResponse(response);
    }
    
    public Task SendAppendEntries(CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}