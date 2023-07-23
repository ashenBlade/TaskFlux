using System.Buffers;
using System.Net.Sockets;
using Consensus.Core;
using Consensus.Core.Commands.Submit;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Packets;
using TaskFlux.Network.Requests.Serialization;

namespace TaskFlux.Host.Modules.BinaryRequest;

internal class RequestProcessor
{
    private readonly TcpClient _client;
    public IConsensusModule<Command, Result> Module { get; }
    public ILogger Logger { get; }
    private readonly PoolingNetworkPacketSerializer _packetSerializer = new(ArrayPool<byte>.Shared);
    public CommandSerializer CommandSerializer { get; } = new();
    public ResultSerializer ResultSerializer { get; } = new();

    public RequestProcessor(TcpClient client, 
                            IConsensusModule<Command, Result> consensusModule,
                            ILogger logger)
    {
        _client = client;
        Module = consensusModule;
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
            while (token.IsCancellationRequested is false && 
                   stream.Socket.Connected)
            {
                var packet = await _packetSerializer.DeserializeAsync(stream, token);
                if (token.IsCancellationRequested)
                {
                    break;
                }

                await packet.AcceptAsync(clientRequestPacketVisitor, token);
            }
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
                responsePacket = new NotLeaderPacket(0);
            }

            // TODO: указывать узел с лидером
            using var pooledResponseArray = _processor._packetSerializer.Serialize(responsePacket);
            await _stream.WriteAsync(pooledResponseArray.ToMemory(), token);
        }

        public async ValueTask VisitAsync(DataResponsePacket packet, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public async ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public async ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }
    }
}