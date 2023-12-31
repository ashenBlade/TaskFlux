using System.Net.Sockets;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Peer.Exceptions;
using Consensus.Raft;
using Consensus.Raft.Commands.InstallSnapshot;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Models;

namespace Consensus.NodeProcessor;

public class NodeConnectionProcessor : IDisposable
{
    public NodeConnectionProcessor(NodeId id,
                                   PacketClient client,
                                   IRaftConsensusModule<Command, Response> raftConsensusModule,
                                   ILogger logger)
    {
        Id = id;
        Client = client;
        RaftConsensusModule = raftConsensusModule;
        Logger = logger;
    }

    public CancellationTokenSource CancellationTokenSource { get; init; } = null!;
    private NodeId Id { get; }
    private Socket Socket => Client.Socket;
    private PacketClient Client { get; }
    private IRaftConsensusModule<Command, Response> RaftConsensusModule { get; }
    private ILogger Logger { get; }

    public async Task ProcessClientBackground()
    {
        var token = CancellationTokenSource.Token;
        Logger.Information("Начинаю обрабатывать запросы клиента {Id}", Id);
        try
        {
            while (token.IsCancellationRequested is false)
            {
                var packet = await ReceivePacketAsync(token);
                if (packet is null)
                {
                    break;
                }

                var success = await ProcessPacket(packet, token);
                if (!success)
                {
                    break;
                }
            }
        }
        catch (IOException io)
        {
            Logger.Information(io, "Соединение с узлом потеряно");
            CloseClient();
        }
        catch (SocketException se)
        {
            Logger.Information(se, "Соединение с узлом потеряно");
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (UnknownPacketException upe)
        {
            Logger.Warning(upe, "От узла получен неожиданный пакет данных");
        }
        catch (Exception e)
        {
            Logger.Warning(e, "Во время обработки узла {Node} возникло необработанное исключение", Id);
        }

        CloseClient();
    }

    private async ValueTask<RaftPacket?> ReceivePacketAsync(CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            try
            {
                return await Client.ReceiveAsync(token);
            }
            catch (IntegrityException)
            {
            }

            Logger.Debug("От узла получен пакет с нарушенной целосностью. Отправляю RetransmitRequest");
            await Client.SendAsync(new RetransmitRequestPacket(), token);
        }
    }

    /// <summary>
    /// Функция для обработки прешдшего пакета
    /// </summary>
    /// <param name="packet">Пришедший пакет запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>
    /// <c>true</c> - запрос успешно обработан <br/>
    /// <c>false</c> - обработка должна закончиться
    /// </returns>
    private async Task<bool> ProcessPacket(RaftPacket packet, CancellationToken token)
    {
        try
        {
            switch (packet.PacketType)
            {
                case RaftPacketType.AppendEntriesRequest:
                    return await ProcessAppendEntriesAsync(( AppendEntriesRequestPacket ) packet, token);
                case RaftPacketType.RequestVoteRequest:
                    return await ProcessRequestVoteAsync(( RequestVoteRequestPacket ) packet, token);
                case RaftPacketType.InstallSnapshotRequest:
                    return await ProcessInstallSnapshotAsync(( InstallSnapshotRequestPacket ) packet, token);
                default:
                    Logger.Error("От клиента получен неожиданный тип пакета: {PacketType}. Закрываю соединение",
                        packet.PacketType);
                    return false;
            }
        }
        catch (Exception e)
        {
            Logger.Error(e,
                "Во время обработки пакета {PacketType} от узла возникло необработанное исключение. Содержимое пакета: {@PacketData}",
                packet.PacketType, packet);
            return false;
        }
    }

    async ValueTask<bool> ProcessAppendEntriesAsync(AppendEntriesRequestPacket packet, CancellationToken token)
    {
        var request = packet.Request;
        var result = RaftConsensusModule.Handle(request);
        try
        {
            await Client.SendAsync(new AppendEntriesResponsePacket(result), token);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private async ValueTask<bool> ProcessRequestVoteAsync(RequestVoteRequestPacket packet, CancellationToken token)
    {
        Logger.Information("От узла получен RequestVote пакет");
        var request = packet.Request;
        var result = RaftConsensusModule.Handle(request);
        await Client.SendAsync(new RequestVoteResponsePacket(result), token);
        return true;
    }

    private async Task<bool> ProcessInstallSnapshotAsync(InstallSnapshotRequestPacket packet, CancellationToken token)
    {
        var snapshot = new NetworkSnapshot(Client);
        var request = new InstallSnapshotRequest(packet.Term, packet.LeaderId, packet.LastEntry, snapshot);
        foreach (var response in RaftConsensusModule.Handle(request, token))
        {
            await Client.SendAsync(new InstallSnapshotResponsePacket(response.CurrentTerm), token);
        }

        return true;
    }

    private void CloseClient()
    {
        if (!Socket.Connected)
        {
            return;
        }

        Logger.Information("Закрываю соединение с узлом");
        Client.Socket.Disconnect(false);
        Client.Socket.Close();
    }

    public void Dispose()
    {
        try
        {
            CancellationTokenSource.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        CloseClient();
        Socket.Dispose();
        CancellationTokenSource.Dispose();
    }
}