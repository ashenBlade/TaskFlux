using System.Buffers;
using System.Net.Sockets;
using Consensus.Core;
using Consensus.Core.Commands.Submit;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;
using TaskFlux.Core;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Packets;
using TaskFlux.Network.Requests.Serialization;

namespace TaskFlux.Host.Modules.BinaryRequest;

internal class RequestProcessor
{
    private readonly TcpClient _client;
    private IConsensusModule<Command, Result> Module { get; }
    public IClusterInfo ClusterInfo { get; }
    private ILogger Logger { get; }
    private readonly PoolingNetworkPacketSerializer _packetSerializer = new(ArrayPool<byte>.Shared);
    private CommandSerializer CommandSerializer { get; } = CommandSerializer.Instance;
    private ResultSerializer ResultSerializer { get; } = ResultSerializer.Instance;

    public RequestProcessor(TcpClient client, 
                            IConsensusModule<Command, Result> consensusModule,
                            IClusterInfo clusterInfo,
                            ILogger logger)
    {
        _client = client;
        Module = consensusModule;
        ClusterInfo = clusterInfo;
        Logger = logger;
    }

    public async Task ProcessAsync(CancellationToken token)
    {
        Logger.Debug("Открываю сетевой поток для коммуницирования к клиентом");
        await using var stream = _client.GetStream();
        var clientRequestPacketVisitor = new ClientRequestPacketVisitor(this, stream);
        try
        {
            Logger.Debug("Начинаю обработку полученного запроса");
            while (token.IsCancellationRequested is false && stream.Socket.Connected)
            {
                var packet = await _packetSerializer.DeserializeAsync(stream, token);
                if (token.IsCancellationRequested)
                {
                    break;
                }

                await packet.AcceptAsync(clientRequestPacketVisitor, token);

                if (clientRequestPacketVisitor.ShouldClose)
                {
                    break;
                }
            }
        }
        catch (Exception e)
        {
            Logger.Error(e, "Во время обработки клиента возникло необработанное исключение");
        }
        finally
        {
            _client.Close();
            _client.Dispose();
        }
    }
    private class ClientRequestPacketVisitor : IAsyncPacketVisitor
    {
        private readonly RequestProcessor _processor;
        private readonly NetworkStream _stream;
        private ILogger Logger => _processor.Logger;
        public bool ShouldClose { get; private set; }
        
        public ClientRequestPacketVisitor(RequestProcessor processor, NetworkStream stream)
        {
            _processor = processor;
            _stream = stream;
        }

        public async ValueTask VisitAsync(DataRequestPacket packet, CancellationToken token = default)
        {
            Command command;
            try
            {
                command = _processor.CommandSerializer.Deserialize(packet.Payload);
            }
            catch (Exception e)
            {
                _processor.Logger.Warning(e, "Ошибка при десерилазации команды от клиента");
                var errorPacket = new ErrorResponsePacket("Ошибка десерализации команды");
                using var p = _processor._packetSerializer.Serialize(errorPacket);
                await _stream.WriteAsync(p.ToMemory(), token);
                return;
            }

            var result = _processor.Module.Handle( 
                new SubmitRequest<Command>(command.Accept(CommandDescriptorBuilderCommandVisitor.Instance)));

            Packet responsePacket;
            if (result.TryGetResponse(out var response))
            {
                responsePacket = new DataResponsePacket(_processor.ResultSerializer.Serialize(response));
            }
            else if (result.WasLeader)
            {
                responsePacket = new DataResponsePacket(Array.Empty<byte>());
            }
            else
            {
                responsePacket = new NotLeaderPacket(_processor.ClusterInfo.LeaderId.Value);
            }

            using var pooledResponseArray = _processor._packetSerializer.Serialize(responsePacket);
            await _stream.WriteAsync(pooledResponseArray.ToMemory(), token);
        }

        public ValueTask VisitAsync(DataResponsePacket packet, CancellationToken token = default)
        {
            Logger.Warning("От клиента пришел неожиданный пакет: DataResponsePacket");
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default)
        {
            Logger.Warning("От клиента пришел неожиданный пакет: DataResponsePacket");
            ShouldClose = true;
            return ValueTask.CompletedTask;        }

        public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default)
        {
            Logger.Warning("От клиента пришел неожиданный пакет: DataResponsePacket");
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }
    }
}