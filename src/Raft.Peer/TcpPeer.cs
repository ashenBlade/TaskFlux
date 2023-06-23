using System.Net.Sockets;
using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Network;
using Raft.Peer.Exceptions;
using Serilog;

namespace Raft.Peer;

public class TcpPeer: IPeer, IDisposable
{
    private readonly ISocket _client;
    private readonly ILogger _logger;
    
    public TcpPeer(NodeId id, ISocket client, ILogger logger)
    {
        _client = client;
        Id = id;
        _logger = logger;
    }

    public NodeId Id { get; }
    
    public async Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        byte[] response;
        var data = Serializers.AppendEntriesRequest.Serialize(request);
        try
        {
            _logger.Verbose("Делаю запрос AppendEntries {@Request} на узел {PeerId}", request, Id);
            await _client.SendAsync(data, token);

            var memory = new MemoryStream();
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            await _client.ReadAsync(memory, token);

            response = memory.ToArray();
        }
        catch (NetworkException)
        {
            _logger.Debug("Ошибка сети во время отправки Heartbeat");
            return null;
        }
        catch (SocketException socket)
        {
            _logger.Warning(socket, "Неизвестная ошибка сокета во время отправки данных к серерву");
            return null;
        }
        
        if (response.Length == 0)
        {
            _logger.Verbose("Узел {Id} вернул пустой ответ. Соединение было разорвано", Id);
            return null;
        }
        
        _logger.Verbose("Ответ от узла {PeerId} получен. Десериализую", Id);
        var appendEntriesResponse = Serializers.AppendEntriesResponse.Deserialize(response);
        _logger.Verbose("Ответ десериализован {Response}", appendEntriesResponse);
        return appendEntriesResponse;
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);
        byte[] response;
        var data = Serializers.RequestVoteRequest.Serialize(request);
        try
        {
            _logger.Verbose("Делаю запрос RequestVote на узел {PeerId}", Id);
            await _client.SendAsync(data, token);

            using var memoryStream = new MemoryStream();
            _logger.Verbose("Запрос отослан. Начинаю принимать ответ от узла {PeerId}", Id);
            await _client.ReadAsync(memoryStream, token);

            response = memoryStream.ToArray();
            
            // Соединение разорвано
        }
        catch (NetworkException)
        {
            // _logger.Debug(networkException, "Во время отправки данных по сокету произошла ошибка сети");
            return null;
        }
        catch (SocketException socket)
        {
            _logger.Warning(socket, "Неизвестная ошибка сокета во время отправки данных к серерву");
            return null;
        }
        
        if (response.Length == 0)
        {
            _logger.Verbose("Узел {Id} вернул пустой ответ. Соединение было разорвано", Id);
            return null;
        }
        _logger.Verbose("Ответ от узла {PeerId} получен. Десериализую", Id);
        return Serializers.RequestVoteResponse.Deserialize(response);
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}