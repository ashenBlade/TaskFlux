using System.Buffers;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using JobQueue.Core.Exceptions;
using Microsoft.Extensions.Options;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.Serialization;
using TaskFlux.Core;
using TaskFlux.Host.Modules.SocketRequest.Exceptions;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Packets.Authorization;
using TaskFlux.Network.Packets.Packets;
using TaskFlux.Network.Packets.Serialization;

namespace TaskFlux.Host.Modules.SocketRequest;

internal class ClientRequestProcessor
{
    private readonly TcpClient _client;
    private readonly IOptionsMonitor<SocketRequestModuleOptions> _options;
    private readonly IApplicationInfo _applicationInfo;
    private readonly IRequestAcceptor _requestAcceptor;
    private IClusterInfo ClusterInfo { get; }
    private ILogger Logger { get; }

    private static CommandSerializer CommandSerializer =>
        CommandSerializer.Instance;

    private static ResultSerializer ResultSerializer =>
        ResultSerializer.Instance;

    public ClientRequestProcessor(TcpClient client,
                                  IRequestAcceptor requestAcceptor,
                                  IOptionsMonitor<SocketRequestModuleOptions> options,
                                  IApplicationInfo applicationInfo,
                                  IClusterInfo clusterInfo,
                                  ILogger logger)
    {
        _client = client;
        _requestAcceptor = requestAcceptor;
        _options = options;
        _applicationInfo = applicationInfo;
        ClusterInfo = clusterInfo;
        Logger = logger;
    }

    public async Task ProcessAsync(CancellationToken token)
    {
        Logger.Debug("Открываю сетевой поток для коммуницирования к клиентом");
        await using var stream = _client.GetStream();
        var serializer = new TaskFluxPacketClient(ArrayPool<byte>.Shared, stream);
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

    private async Task ProcessClientMain(TaskFluxPacketClient client, CancellationToken token)
    {
        var clientRequestPacketVisitor = new ClientCommandRequestPacketVisitor(this, client);
        Logger.Debug("Начинаю обрабатывать клиентские запросы");
        while (token.IsCancellationRequested is false)
        {
            Packet packet;
            try
            {
                packet = await ReceiveNextPacketAsync(client, token);
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

    private async Task<Packet> ReceiveNextPacketAsync(TaskFluxPacketClient client,
                                                      CancellationToken token)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        linkedCts.CancelAfter(_options.CurrentValue.IdleTimeout);
        Logger.Debug("Начинаю принятие пакета от клиента");
        return await client.ReceiveAsync(token);
    }

    private async Task<bool> AcceptClientAsync(TaskFluxPacketClient client,
                                               CancellationToken token)
    {
        try
        {
            await AuthorizeClientAsync(client, token);
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
            var success = await BootstrapClientAsync(client, token);
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
    private async ValueTask<bool> BootstrapClientAsync(TaskFluxPacketClient client,
                                                       CancellationToken token)
    {
        Logger.Debug("Начинаю процесс настройки клиента");
        var packet = await client.ReceiveAsync(token);
        var bootstrapVisitor = new BootstrapPacketAsyncVisitor(client, _applicationInfo);
        await packet.AcceptAsync(bootstrapVisitor, token);
        Logger.Debug("Клиент настроен");
        return !bootstrapVisitor.ShouldClose;
    }

    private class BootstrapPacketAsyncVisitor : IAsyncPacketVisitor
    {
        private readonly TaskFluxPacketClient _client;
        private readonly IApplicationInfo _applicationInfo;
        public bool ShouldClose { get; private set; }

        public BootstrapPacketAsyncVisitor(TaskFluxPacketClient client, IApplicationInfo applicationInfo)
        {
            _client = client;
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
                await _client.SendAsync(BootstrapResponsePacket.Ok, token);
                return;
            }

            await _client.SendAsync(
                BootstrapResponsePacket.Error(
                    $"Версия клиента несовместима с версией сервера. Версия сервера: {_applicationInfo.Version}. Версия клиента: {clientVersion}"),
                token);
            ShouldClose = true;
        }

        public ValueTask VisitAsync(ClusterMetadataResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.ClusterMetadataResponse);
        }

        public ValueTask VisitAsync(ClusterMetadataRequestPacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.ClusterMetadataRequest);
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

    private async Task AuthorizeClientAsync(TaskFluxPacketClient client,
                                            CancellationToken token)
    {
        Logger.Debug("Начинаю процесс авторизации клиента");
        var packet = await client.ReceiveAsync(token);
        var authorizerVisitor = new AuthorizerClientPacketAsyncVisitor(client);
        await packet.AcceptAsync(authorizerVisitor, token);
        Logger.Debug("Клиент авторизовался успешно");
    }

    private class AuthorizerClientPacketAsyncVisitor : IAsyncPacketVisitor
    {
        private readonly TaskFluxPacketClient _client;

        public AuthorizerClientPacketAsyncVisitor(TaskFluxPacketClient client)
        {
            _client = client;
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
            var authVisitor = new AuthorizationFlowMethodVisitor(_client);
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

        public ValueTask VisitAsync(ClusterMetadataResponsePacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.ClusterMetadataResponse);
        }

        public ValueTask VisitAsync(ClusterMetadataRequestPacket packet, CancellationToken token = default)
        {
            return ReturnThrowUnexpectedPacket(PacketType.ClusterMetadataRequest);
        }

        private class AuthorizationFlowMethodVisitor : IAsyncAuthorizationMethodVisitor
        {
            private readonly TaskFluxPacketClient _client;

            public AuthorizationFlowMethodVisitor(TaskFluxPacketClient client)
            {
                _client = client;
            }

            public async ValueTask VisitAsync(NoneAuthorizationMethod noneAuthorizationMethod, CancellationToken token)
            {
                await _client.SendAsync(AuthorizationResponsePacket.Ok, token);
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
        private readonly ClientRequestProcessor _processor;
        private readonly TaskFluxPacketClient _client;
        private ILogger Logger => _processor.Logger;
        public bool ShouldClose { get; private set; }

        public ClientCommandRequestPacketVisitor(ClientRequestProcessor processor,
                                                 TaskFluxPacketClient client)
        {
            _processor = processor;
            _client = client;
        }

        public async ValueTask VisitAsync(CommandRequestPacket packet, CancellationToken token = default)
        {
            Command command;
            try
            {
                command = CommandSerializer.Deserialize(packet.Payload);
            }
            // 1. Чтобы в бизнес-логике каждый раз не писать проверку - проверим здесь
            // 2. Зачем клиенту долго ждать, если такую ошибку можно проверить сразу?
            catch (InvalidQueueNameException invalidName)
            {
                // Это ошибка бизнес-логики, поэтому соединение не разрывается
                // PERF: в статическое поле для оптимизации выделить
                _processor.Logger.Debug(invalidName,
                    "Ошибка при десериализации команды, включающей название очереди, полученной от клиента");
                await _client.SendAsync(
                    new CommandResponsePacket(ResultSerializer.Serialize(DefaultErrors.InvalidQueueName)),
                    token);
                return;
            }
            catch (SerializationException e)
            {
                _processor.Logger.Warning(e, "Ошибка при десерилазации команды от клиента. Закрываю соединение");
                ShouldClose = true;
                return;
            }

            var result = await _processor._requestAcceptor.AcceptAsync(command, token);

            Packet responsePacket;
            if (result.TryGetResponse(out var response))
            {
                responsePacket = new CommandResponsePacket(ResultSerializer.Serialize(response));
            }
            else if (result.WasLeader)
            {
                responsePacket = new CommandResponsePacket(Array.Empty<byte>());
            }
            else
            {
                responsePacket = new NotLeaderPacket(_processor.ClusterInfo.LeaderId is { } id
                                                         ? id.Id
                                                         : null);
            }

            await responsePacket.AcceptAsync(_client, token);
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

        public ValueTask VisitAsync(ClusterMetadataResponsePacket packet, CancellationToken token = default)
        {
            ShouldClose = true;
            return ValueTask.CompletedTask;
        }

        public async ValueTask VisitAsync(ClusterMetadataRequestPacket packet, CancellationToken token = default)
        {
            var data = new ClusterMetadataResponsePacket(_processor.ClusterInfo.Nodes,
                GetId(_processor.ClusterInfo.LeaderId), _processor.ClusterInfo.CurrentNodeId.Id);
            await _client.SendAsync(data, token);
        }
    }

    private static int? GetId(NodeId? id)
    {
        return id is { } i
                   ? i.Id
                   : null;
    }
}