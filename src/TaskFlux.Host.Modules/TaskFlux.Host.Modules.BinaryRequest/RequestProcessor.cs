using System.Buffers;
using System.Net.Sockets;
using Consensus.Core;
using Consensus.Core.Commands.Submit;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Serialization;
using TaskFlux.Core;
using TaskFlux.Host.Modules.BinaryRequest.Exceptions;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Authorization;
using TaskFlux.Network.Requests.Packets;
using TaskFlux.Network.Requests.Serialization;

namespace TaskFlux.Host.Modules.BinaryRequest;

internal class RequestProcessor
{
    private readonly TcpClient _client;
    private IConsensusModule<Command, Result> Module { get; }
    public IClusterInfo ClusterInfo { get; }
    private ILogger Logger { get; }
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
        var serializer = new PoolingNetworkPacketSerializer(ArrayPool<byte>.Shared, stream);
        try
        {
            try
            {
                await AuthorizeClientAsync(serializer, token);
            }
            catch (UnexpectedPacketException unexpected)
            {
                Logger.Warning(unexpected, "От клиента пришел неожиданный пакет");
                return;
            }
        
            var clientRequestPacketVisitor = new ClientRequestPacketVisitor(this, serializer);
            Logger.Debug("Начинаю обрабатывать клиентские запросы");
            while (token.IsCancellationRequested is false && 
                   stream.Socket.Connected)
            {
                var packet = await serializer.DeserializeAsync(token);
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

    private async Task AuthorizeClientAsync(PoolingNetworkPacketSerializer serializer,
                                            CancellationToken token)
    {
        Logger.Debug("Начинаю процесс авторизации клиента");
        var packet = await serializer.DeserializeAsync(token);
        var authorizerVisitor = new AuthorizerClientPacketAsyncVisitor(serializer);
        await packet.AcceptAsync(authorizerVisitor, token);
        Logger.Debug("Клиент авторизовался успешно");
    }

    private class AuthorizerClientPacketAsyncVisitor : IAsyncPacketVisitor
    {
        private readonly PoolingNetworkPacketSerializer _serializer;

        public AuthorizerClientPacketAsyncVisitor(PoolingNetworkPacketSerializer serializer)
        {
            _serializer = serializer;
        }
        
        public ValueTask VisitAsync(CommandRequestPacket packet, CancellationToken token = default)
        {
            return ValueTask.FromException(new UnexpectedPacketException(PacketType.CommandRequest));
        }

        public ValueTask VisitAsync(CommandResponsePacket packet, CancellationToken token = default)
        {
            return ValueTask.FromException(new UnexpectedPacketException(PacketType.CommandResponse));
        }

        public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default)
        {
            return ValueTask.FromException(new UnexpectedPacketException(PacketType.ErrorResponse));
        }

        public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default)
        {
            return ValueTask.FromException(new UnexpectedPacketException(PacketType.ErrorResponse));
        }

        public async ValueTask VisitAsync(AuthorizationRequestPacket packet, CancellationToken token = default)
        {
            var authVisitor = new AuthorizationFlowMethodVisitor(_serializer);
            await packet.AuthorizationMethod.AcceptAsync(authVisitor, token);
        }

        public ValueTask VisitAsync(AuthorizationResponsePacket packet, CancellationToken token = default)
        {
            return ValueTask.FromException(new UnexpectedPacketException(PacketType.AuthorizationResponse));
        }

        private class AuthorizationFlowMethodVisitor : IAsyncAuthorizationMethodVisitor
        {
            private readonly PoolingNetworkPacketSerializer _serializer;

            public AuthorizationFlowMethodVisitor(PoolingNetworkPacketSerializer serializer)
            {
                _serializer = serializer;
            }
            
            public ValueTask VisitAsync(NoneAuthorizationMethod noneAuthorizationMethod, CancellationToken token)
            {
                return _serializer.SerializeAsync(AuthorizationResponsePacket.Ok, token);
            }
        }
    }

    private class ClientRequestPacketVisitor : IAsyncPacketVisitor
    {
        private readonly RequestProcessor _processor;
        private readonly PoolingNetworkPacketSerializer _serializer;
        private ILogger Logger => _processor.Logger;
        public bool ShouldClose { get; private set; }
        
        public ClientRequestPacketVisitor(RequestProcessor processor,
                                          PoolingNetworkPacketSerializer serializer)
        {
            _processor = processor;
            _serializer = serializer;
        }

        public async ValueTask VisitAsync(CommandRequestPacket packet, CancellationToken token = default)
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
                await _serializer.VisitAsync(errorPacket, token);
                return;
            }

            var result = _processor.Module.Handle( 
                new SubmitRequest<Command>(command.Accept(CommandDescriptorBuilderCommandVisitor.Instance)));

            Packet responsePacket;
            if (result.TryGetResponse(out var response))
            {
                responsePacket = new CommandResponsePacket(_processor.ResultSerializer.Serialize(response));
            }
            else if (result.WasLeader)
            {
                responsePacket = new CommandResponsePacket(Array.Empty<byte>());
            }
            else
            {
                responsePacket = new NotLeaderPacket(_processor.ClusterInfo.LeaderId.Value);
            }

            await responsePacket.AcceptAsync(_serializer, token);
        }

        public ValueTask VisitAsync(CommandResponsePacket packet, CancellationToken token = default)
        {
            Logger.Warning("От клиента пришел неожиданный пакет: DataResponsePacket");
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default)
        {
            Logger.Warning("От клиента пришел неожиданный пакет: DataResponsePacket");
            ShouldClose = true;
            return ValueTask.CompletedTask;        
        }

        public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default)
        {
            Logger.Warning("От клиента пришел неожиданный пакет: DataResponsePacket");
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }

        public async ValueTask VisitAsync(AuthorizationRequestPacket packet, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }

        public async ValueTask VisitAsync(AuthorizationResponsePacket packet, CancellationToken token = default)
        {
            throw new NotImplementedException();
        }
    }
}