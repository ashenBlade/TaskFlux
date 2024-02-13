using System.Net.Sockets;
using Serilog;
using TaskFlux.Application.Cluster.Network;
using TaskFlux.Application.Cluster.Network.Exceptions;
using TaskFlux.Application.Cluster.Network.Packets;
using TaskFlux.Consensus;
using TaskFlux.Consensus.Cluster.Network.Exceptions;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Core;
using TaskFlux.Core.Commands;

namespace TaskFlux.Application.Cluster;

public class NodeConnectionProcessor(
    NodeId id,
    PacketClient client,
    RaftConsensusModule<Command, Response> raftConsensusModule,
    ILogger logger) : IDisposable
{
    public CancellationTokenSource CancellationTokenSource { get; init; } = null!;
    private NodeId Id { get; } = id;
    private Socket Socket => Client.Socket;
    private PacketClient Client { get; } = client;
    private RaftConsensusModule<Command, Response> RaftConsensusModule { get; } = raftConsensusModule;
    private ILogger Logger { get; } = logger;
    private volatile bool _disposed = false;

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
        }
        catch (SocketException se) when (se.SocketErrorCode is SocketError.Interrupted or SocketError.InvalidArgument)
        {
            Logger.Information(se, "Закрываю соединение с узлом");
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

        // CloseClient();
    }

    private async ValueTask<NodePacket?> ReceivePacketAsync(CancellationToken token)
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
    private async Task<bool> ProcessPacket(NodePacket packet, CancellationToken token)
    {
        try
        {
            switch (packet.PacketType)
            {
                case NodePacketType.AppendEntriesRequest:
                    return await ProcessAppendEntriesAsync(( AppendEntriesRequestPacket ) packet, token);
                case NodePacketType.RequestVoteRequest:
                    return await ProcessRequestVoteAsync(( RequestVoteRequestPacket ) packet, token);
                case NodePacketType.InstallSnapshotRequest:
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
        var response = RaftConsensusModule.Handle(request, token);
        await Client.SendAsync(new InstallSnapshotResponsePacket(response.CurrentTerm), token);
        return true;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        Socket.Dispose();
        CancellationTokenSource.Dispose();
    }

    public void Stop()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            CancellationTokenSource.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            Socket.Shutdown(SocketShutdown.Both);
        }
        catch (SocketException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }
}