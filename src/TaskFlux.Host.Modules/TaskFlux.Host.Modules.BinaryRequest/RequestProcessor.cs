using System.Buffers;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Consensus.Raft;
using Consensus.Raft.Commands.Submit;
using JobQueue.Core.Exceptions;
using Microsoft.Extensions.Options;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Error;
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
    private readonly IOptionsMonitor<BinaryRequestModuleOptions> _options;
    private readonly IApplicationInfo _applicationInfo;
    private IConsensusModule<Command, Result> Module { get; }
    public IClusterInfo ClusterInfo { get; }
    private ILogger Logger { get; }
    private CommandSerializer CommandSerializer { get; } = CommandSerializer.Instance;
    private ResultSerializer ResultSerializer { get; } = ResultSerializer.Instance;

    public RequestProcessor(TcpClient client, 
                            IConsensusModule<Command, Result> consensusModule,
                            IOptionsMonitor<BinaryRequestModuleOptions> options,
                            IApplicationInfo applicationInfo,
                            IClusterInfo clusterInfo,
                            ILogger logger)
    {
        _client = client;
        _options = options;
        _applicationInfo = applicationInfo;
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
            var success = await AcceptClientAsync(serializer, token)
                             .ConfigureAwait(false);
            
            if (!success)
            {
                return;
            }
            
            await ProcessClientMain(serializer, token);
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

    private async Task ProcessClientMain(PoolingNetworkPacketSerializer serializer, CancellationToken token)
    {
        var clientRequestPacketVisitor = new ClientCommandRequestPacketVisitor(this, serializer);
        Logger.Debug("Начинаю обрабатывать клиентские запросы");
        while (token.IsCancellationRequested is false)
        {
            Packet packet;
            try
            {
                packet = await ReceiveNextPacketAsync(serializer, token);
            }
            catch (OperationCanceledException canceled)
                when (token.IsCancellationRequested)
            {
                Logger.Information(canceled, "Запрошено завершение работы во время обработки клиента");
                return;
            }
            catch (OperationCanceledException canceled)
            {
                Logger.Information(canceled, "Превышен таймаут ожидания пакета от клиента. Завершаю обработку");
                return;
            }
        
            await packet.AcceptAsync(clientRequestPacketVisitor, token);
        
            if (clientRequestPacketVisitor.ShouldClose)
            {
                break;
            }
        }
    }

    private async Task<Packet> ReceiveNextPacketAsync(PoolingNetworkPacketSerializer serializer,
                                                      CancellationToken token)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        linkedCts.CancelAfter(_options.CurrentValue.IdleTimeout);
        Logger.Debug("Начинаю принятие пакета от клиента");
        return await serializer.DeserializeAsync(token);
    }

    private async Task<bool> AcceptClientAsync(PoolingNetworkPacketSerializer serializer,
                                               CancellationToken token)
    {
        try
        {
            await AuthorizeClientAsync(serializer, token);
        }
        catch (UnexpectedPacketException unexpected)
        {
            Logger.Warning(unexpected, "От клиента пришел неожиданный пакет");
            return false;
        }
        catch (Exception e)
        {
            Logger.Warning(e, "Неизвестная ошибка во время авторизации клиента");
            return false;
        }
        
        try
        {
            var success = await BootstrapClientAsync(serializer, token);
            if (!success)
            {
                return false;
            }
        }
        catch (Exception e)
        {
            Logger.Warning(e, "Во время настроки клиента поймано необработанное исключение");
            return false;
        }
        
        return true;
    }
    
    /// <summary>
    /// Метод для запуска процесса настройки клиента и сервера
    /// </summary>
    /// <returns><c>true</c> - клиент успешно настроен<br/> <c>false</c> - во время настройки возникла ошибка</returns>
    private async ValueTask<bool> BootstrapClientAsync(PoolingNetworkPacketSerializer serializer, CancellationToken token)
    {
        Logger.Debug("Начинаю процесс настройки клиента");
        var packet = await serializer.DeserializeAsync(token);
        var bootstrapVisitor = new BootstrapPacketAsyncVisitor(serializer, _applicationInfo);
        await packet.AcceptAsync(bootstrapVisitor, token);
        Logger.Debug("Клиент настроен");
        return !bootstrapVisitor.ShouldClose;
    }

    private class BootstrapPacketAsyncVisitor : IAsyncPacketVisitor
    {
        private readonly PoolingNetworkPacketSerializer _serializer;
        private readonly IApplicationInfo _applicationInfo;
        public bool ShouldClose { get; private set; }

        public BootstrapPacketAsyncVisitor(PoolingNetworkPacketSerializer serializer, IApplicationInfo applicationInfo)
        {
            _serializer = serializer;
            _applicationInfo = applicationInfo;
        }
        public ValueTask VisitAsync(CommandRequestPacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.CommandRequest);
        }

        public ValueTask VisitAsync(CommandResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.CommandResponse);
        }

        public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.ErrorResponse);
        }

        public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.NotLeader);
        }

        public ValueTask VisitAsync(AuthorizationRequestPacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.AuthorizationRequest);
        }

        public ValueTask VisitAsync(AuthorizationResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.AuthorizationResponse);
        }

        public ValueTask VisitAsync(BootstrapResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.BootstrapResponse);
        }

        public async ValueTask VisitAsync(BootstrapRequestPacket packet, CancellationToken token = default)
        {
            var clientVersion = new Version(packet.Major, packet.Minor, packet.Patch);
            if (IsCompatibleWith(clientVersion))
            {
                await _serializer.SerializeAsync(BootstrapResponsePacket.Ok, token);
                return;
            }

            await _serializer.SerializeAsync(BootstrapResponsePacket.Error(
                $"Версия клиента несовместима с версией сервера. Версия сервера: {_applicationInfo.Version}. Версия клиента: {clientVersion}"), token);
            ShouldClose = true;
        }

        private bool IsCompatibleWith(Version clientVersion)
        {
            var serverVersion = _applicationInfo.Version;
            if (serverVersion.Major != clientVersion.Major)
            {
                return false;
            }

            return serverVersion.Minor <= clientVersion.Minor;
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
            return ReturnThrowUnexpectedPacket(PacketType.CommandRequest);
        }

        public ValueTask VisitAsync(CommandResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.CommandResponse);
        }

        public ValueTask VisitAsync(ErrorResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.ErrorResponse);
        }

        public ValueTask VisitAsync(NotLeaderPacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.ErrorResponse);
        }

        public async ValueTask VisitAsync(AuthorizationRequestPacket packet, CancellationToken token = default)
        {
            var authVisitor = new AuthorizationFlowMethodVisitor(_serializer);
            await packet.AuthorizationMethod.AcceptAsync(authVisitor, token);
        }

        public ValueTask VisitAsync(AuthorizationResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.AuthorizationResponse);
        }

        public ValueTask VisitAsync(BootstrapResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.BootstrapResponse);
        }

        public ValueTask VisitAsync(BootstrapRequestPacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.BootstrapRequest);
        }

        private class AuthorizationFlowMethodVisitor : IAsyncAuthorizationMethodVisitor
        {
            private readonly PoolingNetworkPacketSerializer _serializer;

            public AuthorizationFlowMethodVisitor(PoolingNetworkPacketSerializer serializer)
            {
                _serializer = serializer;
            }
            
            public async ValueTask VisitAsync(NoneAuthorizationMethod noneAuthorizationMethod, CancellationToken token)
            {
                await _serializer.SerializeAsync(AuthorizationResponsePacket.Ok, token);
            }
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask ReturnThrowUnexpectedPacket(PacketType actualType)
    {
        return ValueTask.FromException(new UnexpectedPacketException(actualType));
    }

    private class ClientCommandRequestPacketVisitor : IAsyncPacketVisitor
    {
        private readonly RequestProcessor _processor;
        private readonly PoolingNetworkPacketSerializer _serializer;
        private ILogger Logger => _processor.Logger;
        public bool ShouldClose { get; private set; }
        
        public ClientCommandRequestPacketVisitor(RequestProcessor processor,
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
            // 1. Чтобы в бизнес-логике каждый раз не писать проверку - проверим здесь
            // 2. Зачем клиенту долго ждать, если такую ошибку можно проверить сразу?
            catch (InvalidQueueNameException invalidName)
            {
                // Это ошибка бизнес-логики, поэтому соединение не разрывается
                // PERF: в статическое поле для оптимизации выделить
                _processor.Logger.Debug(invalidName, "Ошибка при десериализации команды, включающей название очереди, полученной от клиента");
                    await _serializer.SerializeAsync(
                    new CommandResponsePacket(_processor.ResultSerializer.Serialize(DefaultErrors.InvalidQueueName)), token);
                return;
            }
            catch (SerializationException e)
            {
                _processor.Logger.Warning(e, "Ошибка при десерилазации команды от клиента. Закрываю соединение");
                ShouldClose = true;
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

        public ValueTask VisitAsync(AuthorizationRequestPacket packet, CancellationToken token = default)
        {
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask VisitAsync(AuthorizationResponsePacket packet, CancellationToken token = default)
        {
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask VisitAsync(BootstrapResponsePacket packet, CancellationToken token = default)
        {
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask VisitAsync(BootstrapRequestPacket packet, CancellationToken token = default)
        {
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }
    }
}