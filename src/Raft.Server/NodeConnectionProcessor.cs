using System.Net.Sockets;
using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Node;
using Raft.Network;
using Raft.Network.Packets;
using Raft.Network.Socket;
using Serilog;

namespace Raft.Server;

public class NodeConnectionProcessor : IDisposable
{
    public NodeConnectionProcessor(NodeId id, IRemoteNodeConnection connection, TcpClient client, RaftNode node, ILogger logger)
    {
        Id = id;
        Connection = connection;
        Client = client;
        Node = node;
        Logger = logger;
    }

    public CancellationTokenSource CancellationTokenSource { get; init; } = null!;
    public NodeId Id { get; init; }
    public TcpClient Client { get; set; }
    public IRemoteNodeConnection Connection { get; init; }
    public RaftNode Node { get; init; }
    public ILogger Logger { get; init; }

    public async Task ProcessClientBackground()
    {
        var token = CancellationTokenSource.Token;
        var connection = Connection;
        Logger.Debug("Начинаю обрабатывать запросы клиента {Id}", Id);
        while (token.IsCancellationRequested is false)
        {
            var packet = await connection.ReceiveAsync(token);
            if (packet is null)
            {
                Logger.Information("От узла пришел пустой пакет. Соединение разорвано. Прекращаю обработку");
                break;
            }

            Logger.Debug("От клиента получен пакет {Packet}", packet);
            var success = await ProcessPacket(packet, token);
            if (!success)
            {
                break;
            }
        }
    }

    private async Task<bool> ProcessPacket(IPacket packet, CancellationToken token)
    {
        switch (packet.PacketType)
        {
            case PacketType.AppendEntriesRequest:
                await ProcessAppendEntriesAsync();
                break;
            case PacketType.RequestVoteRequest:
                await ProcessRequestVoteAsync();
                break;
            default:
                Logger.Information("От клиента получен неожиданный тип пакета: {PacketType}. Закрываю соединение", packet.PacketType);
                return false;
        }

        return true;

        async Task ProcessAppendEntriesAsync()
        {
            var request = ( ( AppendEntriesRequestPacket ) packet ).Request;
            var result = Node.Handle(request);
            Logger.Debug("Запрос обработан: {Result}", result);
            await Connection.SendAsync(new AppendEntriesResponsePacket(result), token);
        }

        async Task ProcessRequestVoteAsync()
        {
            var request = ((RequestVoteRequestPacket ) packet ).Request;
            var result = Node.Handle(request);
            Logger.Debug("Запрос обработан: {Result}", result);
            await Connection.SendAsync(new RequestVoteResponsePacket(result), token);
        }
    }

    public void Dispose()
    {
        try
        {
            CancellationTokenSource.Cancel();
        }
        catch (ObjectDisposedException)
        { }

        Connection.Dispose();
        CancellationTokenSource.Dispose();
    }
}