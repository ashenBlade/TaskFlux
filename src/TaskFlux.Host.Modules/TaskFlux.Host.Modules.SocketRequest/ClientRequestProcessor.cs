using System.ComponentModel;
using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Core;
using TaskFlux.Host.Modules.SocketRequest.Exceptions;
using TaskFlux.Models.Exceptions;
using TaskFlux.Network.Client;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Packets.Authorization;
using TaskFlux.Network.Packets.Exceptions;
using TaskFlux.Network.Packets.Packets;
using TaskFlux.Network.Packets.Responses;

namespace TaskFlux.Host.Modules.SocketRequest;

internal class ClientRequestProcessor
{
    private readonly TcpClient _client;
    private readonly IOptionsMonitor<SocketRequestModuleOptions> _options;
    private readonly IApplicationInfo _applicationInfo;
    private readonly IRequestAcceptor _requestAcceptor;
    private readonly IClusterInfo _clusterInfo;
    private readonly ILogger _logger;

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
        _clusterInfo = clusterInfo;
        _logger = logger;
    }

    public async Task ProcessAsync(CancellationToken token)
    {
        // 1. Авторизуем клиента
        // 2. Настраиваем клиента
        // 3. Обрабатываем его запросы (Enqueue, Dequeue, Create, ...)
        _logger.Information("Начинаю обработку клиента");
        await using var stream = _client.GetStream();
        var client = new TaskFluxClient(stream);
        try
        {
            if (await AcceptSetupClientAsync(client, token))
            {
                await ProcessClientForeverAsync(client, token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (EndOfStreamException)
        {
            _logger.Information("Клиент закрыл соединение");
        }
        catch (UnknownAuthorizationMethodException uame)
        {
            _logger.Warning(uame, "От клиента получен неизвестный метод авторизации");
        }
        catch (UnknownCommandTypeException ucte)
        {
            _logger.Warning(ucte, "От клиента получена неизвестная команда");
        }
        catch (UnknownResponseTypeException urte)
        {
            _logger.Warning(urte, "От клиента получен пакет ответа");
        }
        catch (UnknownPacketException upe)
        {
            _logger.Warning(upe, "От клиента получен неизвестный пакет");
        }
        catch (IOException io)
        {
            _logger.Warning(io, "Ошибка во время коммуникации с клиентом");
        }
        catch (SocketException se)
        {
            _logger.Warning(se, "Ошибка во время коммуникации с клиентом");
        }
        catch (Exception e)
        {
            _logger.Error(e, "Во время обработки клиента произошла неизвестная ошибка");
        }
    }

    /// <summary>
    /// Принять и настроить клиента
    /// </summary>
    /// <returns><c>true</c> - клиент настроен успешно, <c>false</c> - иначе</returns>
    private async Task<bool> AcceptSetupClientAsync(TaskFluxClient client, CancellationToken token)
    {
        // 1. Авторизация
        var request = await client.ReceiveAsync(token);
        Debug.Assert(Enum.IsDefined(request.Type), "Неизвестный тип пакета",
            "От клиента получен неизвестный тип пакета {0}: {1}", request.Type, request);

        AuthorizationRequestPacket authorizationRequest;
        switch (request.Type)
        {
            case PacketType.AuthorizationRequest:
                authorizationRequest = ( AuthorizationRequestPacket ) request;
                break;
            case PacketType.CommandRequest:
            case PacketType.CommandResponse:
            case PacketType.AcknowledgeRequest:
            case PacketType.ErrorResponse:
            case PacketType.NotLeader:
            case PacketType.AuthorizationResponse:
            case PacketType.BootstrapResponse:
            case PacketType.ClusterMetadataResponse:
            case PacketType.ClusterMetadataRequest:
                _logger.Warning("От клиента получен неожиданный пакет. Ожидался пакет авторизации. Получен: {Packet}",
                    request.Type);
                return false;
            default:
                throw new InvalidEnumArgumentException(nameof(request.Type), ( int ) ( request.Type ),
                    typeof(PacketType));
        }

        var authorizationType = authorizationRequest.AuthorizationMethod.AuthorizationMethodType;
        switch (authorizationType)
        {
            case AuthorizationMethodType.None:
                // Единственный пока метод
                break;
            default:
                throw new InvalidEnumArgumentException(nameof(authorizationType),
                    ( int ) authorizationType,
                    typeof(AuthorizationMethodType));
        }

        await client.SendAsync(AuthorizationResponsePacket.Ok, token);

        // 2. Бутстрап
        request = await client.ReceiveAsync(token);
        Debug.Assert(Enum.IsDefined(request.Type), "Неизвестный тип пакета",
            "От клиента получен неизвестный тип пакета {0}: {1}", request.Type, request);
        BootstrapRequestPacket bootstrapRequest;
        switch (request.Type)
        {
            case PacketType.BootstrapRequest:
                bootstrapRequest = ( BootstrapRequestPacket ) request;
                break;
            case PacketType.CommandRequest:
            case PacketType.CommandResponse:
            case PacketType.AcknowledgeRequest:
            case PacketType.ErrorResponse:
            case PacketType.NotLeader:
            case PacketType.AuthorizationRequest:
            case PacketType.AuthorizationResponse:
            case PacketType.BootstrapResponse:
            case PacketType.ClusterMetadataRequest:
            case PacketType.ClusterMetadataResponse:
                _logger.Warning("От клиента получен неожиданный пакет {PacketType}", request.Type);
                return false;
            default:
                throw new ArgumentOutOfRangeException(nameof(request.Type), request.Type,
                    "От клиента получен неизвестный пакет");
        }

        if (!ValidateVersion(bootstrapRequest))
        {
            await client.SendAsync(BootstrapResponsePacket.Error("Версия клиента не поддерживается"), token);
            return false;
        }

        await client.SendAsync(BootstrapResponsePacket.Ok, token);
        return true;

        bool ValidateVersion(BootstrapRequestPacket packet)
        {
            var serverVersion = _applicationInfo.Version;
            if (serverVersion.Major != packet.Major)
            {
                return false;
            }

            if (serverVersion.Minor < packet.Minor)
            {
                return false;
            }

            // Патч не проверять - там должны быть только баги
            return true;
        }
    }

    private async Task ProcessClientForeverAsync(TaskFluxClient client, CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            var request = await client.ReceiveAsync(token);
            Debug.Assert(Enum.IsDefined(request.Type), "Получен неизвестный тип пакета", "Тип пакета {0} неизвестен",
                request.Type);

            switch (request.Type)
            {
                case PacketType.CommandRequest:
                    await ProcessCommandRequestAsync(client, ( CommandRequestPacket ) request, token);
                    break;
                case PacketType.ClusterMetadataRequest:
                    await ProcessClusterMetadataRequestAsync(client, ( ClusterMetadataRequestPacket ) request, token);
                    break;
                case PacketType.AcknowledgeRequest:
                case PacketType.CommandResponse:
                case PacketType.ErrorResponse:
                case PacketType.NotLeader:
                case PacketType.AuthorizationRequest:
                case PacketType.AuthorizationResponse:
                case PacketType.BootstrapRequest:
                case PacketType.BootstrapResponse:
                case PacketType.ClusterMetadataResponse:
                    _logger.Warning("От клиента получен неожиданный тип пакета {PacketType}", request.Type);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(request.Type), ( int ) request.Type,
                        "Получен неизвестный тип пакета");
            }
        }
    }

    private async Task ProcessClusterMetadataRequestAsync(TaskFluxClient client,
                                                          ClusterMetadataRequestPacket request,
                                                          CancellationToken token)
    {
        var response = new ClusterMetadataResponsePacket(_clusterInfo.Nodes, _clusterInfo.LeaderId?.Id,
            _clusterInfo.CurrentNodeId.Id);
        await client.SendAsync(response, token);
    }

    private async Task ProcessCommandRequestAsync(TaskFluxClient client,
                                                  CommandRequestPacket request,
                                                  CancellationToken token)
    {
        if (_clusterInfo.LeaderId != _clusterInfo.CurrentNodeId)
        {
            await client.SendAsync(new NotLeaderPacket(_clusterInfo.LeaderId?.Id), token);
            return;
        }

        Command command;
        try
        {
            command = CommandMapper.Map(request.Command);
        }
        // TODO: дополнительные исключения для разных типов ошибок бизнес-логики (маппинг)
        catch (InvalidQueueNameException)
        {
            await client.SendAsync(new ErrorResponsePacket(( int ) ErrorCodes.InvalidQueueName, string.Empty), token);
            return;
        }

        switch (command.Type)
        {
            // Для Enqueue и Dequeue отдельные пути обработки (с подтверждениями)
            case CommandType.Enqueue:
                await ProcessEnqueueCommandAsync(client, ( EnqueueCommand ) command, token);
                return;
            case CommandType.Dequeue:
                await ProcessDequeueCommandAsync(client, ( DequeueCommand ) command, token);
                return;
            case CommandType.Count:
            case CommandType.CreateQueue:
            case CommandType.DeleteQueue:
            case CommandType.ListQueues:
                break;
        }

        // Обычный путь команды - Запрос -> Ответ
        var submitResponse = await _requestAcceptor.AcceptAsync(command, token);
        if (submitResponse.TryGetResponse(out var response))
        {
            await client.SendAsync(new CommandResponsePacket(ResponseMapper.Map(response)), token);
        }
        else
        {
            // Если ответа нет, то значит мы не лидер, и команды обрабатывать не можем 
            await client.SendAsync(new NotLeaderPacket(_clusterInfo.LeaderId?.Id ?? null), token);
        }
    }

    private async Task ProcessEnqueueCommandAsync(TaskFluxClient client,
                                                  EnqueueCommand command,
                                                  CancellationToken token)
    {
        /*
         * -> CommandRequest(Enqueue)
         * <- Ok
         * -> Acknowledge
         * ... Вставка
         * <- CommandResponse(Enqueue)
         */

        await client.SendAsync(new CommandResponsePacket(OkNetworkResponse.Instance), token);
        var ackPacket = await client.ReceiveAsync(token);
        Debug.Assert(Enum.IsDefined(ackPacket.Type), "Enum.IsDefined(ackPacket.Type)", "Неизвестный тип пакета: {0}",
            ackPacket.Type);
        switch (ackPacket.Type)
        {
            case PacketType.AcknowledgeRequest:
                break;
            case PacketType.CommandRequest:
            case PacketType.CommandResponse:
            case PacketType.ErrorResponse:
            case PacketType.NotLeader:
            case PacketType.AuthorizationRequest:
            case PacketType.AuthorizationResponse:
            case PacketType.BootstrapRequest:
            case PacketType.BootstrapResponse:
            case PacketType.ClusterMetadataRequest:
            case PacketType.ClusterMetadataResponse:
                throw new UnexpectedPacketException(ackPacket, PacketType.AcknowledgeRequest);
            default:
                throw new ArgumentOutOfRangeException(nameof(ackPacket.Type), ackPacket.Type, "Неизвестный тип пакета");
        }

        var submitResponse = await _requestAcceptor.AcceptAsync(command, token);
        if (submitResponse.TryGetResponse(out var response))
        {
            await client.SendAsync(new CommandResponsePacket(ResponseMapper.Map(response)), token);
        }
        else
        {
            await client.SendAsync(new NotLeaderPacket(_clusterInfo.LeaderId?.Id), token);
        }
    }

    private async Task ProcessDequeueCommandAsync(TaskFluxClient client,
                                                  DequeueCommand command,
                                                  CancellationToken token)
    {
        throw new NotImplementedException();
    }
}