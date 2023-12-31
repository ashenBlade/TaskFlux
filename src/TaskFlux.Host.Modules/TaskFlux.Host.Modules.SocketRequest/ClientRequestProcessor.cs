using System.ComponentModel;
using System.Diagnostics;
using System.Net.Sockets;
using Consensus.Core.Submit;
using Microsoft.Extensions.Options;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Core;
using TaskFlux.Host.Modules.SocketRequest.Exceptions;
using TaskFlux.Models.Exceptions;
using TaskFlux.Network;
using TaskFlux.Network.Authorization;
using TaskFlux.Network.Commands;
using TaskFlux.Network.Exceptions;
using TaskFlux.Network.Packets;

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
        catch (OperationCanceledException)
        {
            _logger.Information("Таймаут ожидания клиента превышен");
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
        Packet request;
        using (var cts = CreateIdleTimeoutCts(token))
        {
            request = await client.ReceiveAsync(cts.Token);
        }

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
                // Единственный пока метод авторизации
                break;
            default:
                throw new InvalidEnumArgumentException(nameof(authorizationType),
                    ( int ) authorizationType,
                    typeof(AuthorizationMethodType));
        }

        await client.SendAsync(AuthorizationResponsePacket.Ok, token);

        // 2. Бутстрап
        using (var cts = CreateIdleTimeoutCts(token))
        {
            request = await client.ReceiveAsync(cts.Token);
        }

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
            Packet request;
            using (var cts = CreateIdleTimeoutCts(token))
            {
                request = await client.ReceiveAsync(cts.Token);
            }

            switch (request.Type)
            {
                case PacketType.CommandRequest:
                    await ProcessCommandRequestAsync(client, ( CommandRequestPacket ) request, token);
                    break;
                case PacketType.ClusterMetadataRequest:
                    await ProcessClusterMetadataRequestAsync(client, token);
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
        switch (request.Command.Type)
        {
            case NetworkCommandType.Enqueue:
                await ProcessEnqueueCommandAsync(client, request, token);
                return;
            case NetworkCommandType.Dequeue:
                await ProcessDequeueCommandAsync(client, request, token);
                return;
            case NetworkCommandType.Count:
            case NetworkCommandType.CreateQueue:
            case NetworkCommandType.DeleteQueue:
            case NetworkCommandType.ListQueues:
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        Command command;
        try
        {
            command = CommandMapper.Map(request.Command);
        }
        catch (InvalidQueueNameException)
        {
            await client.SendAsync(new ErrorResponsePacket(( int ) ErrorCodes.InvalidQueueName, string.Empty), token);
            return;
        }

        // Обычный путь команды - Запрос -> Ответ
        var submitResponse = await _requestAcceptor.AcceptAsync(command, token);
        if (submitResponse.TryGetResponse(out var response))
        {
            _logger.Debug("Результат работы команды: {@Response}", response);
            await client.SendAsync(ResponsePacketMapper.MapResponse(response), token);
        }
        else
        {
            // Если ответа нет, то значит мы не лидер, и команды обрабатывать не можем 
            await client.SendAsync(new NotLeaderPacket(_clusterInfo.LeaderId?.Id ?? null), token);
        }
    }

    private async Task ProcessEnqueueCommandAsync(TaskFluxClient client,
                                                  CommandRequestPacket request,
                                                  CancellationToken token)
    {
        /*
         * Команда вставки разделена на 2 этапа:
         * 1. Клиент отправляет пакет с сообщением, который хочет вставить -> Мы его принимаем
         * 2. Клиент подтверждает вставку (Ack)
         *
         * Мы отправляем OK сразу после маппинга, т.к. на этот момент весь пакет отправленный клиентом получили.
         * Без подобного подтверждения может возникнуть ситуация, когда на момент полного получения пакета с сообщением,
         * клиент отвалился (например, он послал запись в 1Мб и он раздробился на 1500б по Ethernet).
         */
        Command command;
        try
        {
            command = CommandMapper.Map(request.Command);
        }
        catch (InvalidQueueNameException)
        {
            // Указанное пользователем название очереди невалидное
            await client.SendAsync(new ErrorResponsePacket(( int ) ErrorCodes.InvalidQueueName, string.Empty), token);
            return;
        }

        await client.SendAsync(OkPacket.Instance, token);

        Packet ackPacket;
        using (var cts = CreateIdleTimeoutCts(token))
        {
            ackPacket = await client.ReceiveAsync(cts.Token);
        }

        Debug.Assert(Enum.IsDefined(ackPacket.Type), "Enum.IsDefined(ackPacket.Type)", "Неизвестный тип пакета: {0}",
            ackPacket.Type);

        switch (ackPacket.Type)
        {
            case PacketType.AcknowledgeRequest:
                break;
            case PacketType.NegativeAcknowledgementRequest:
                // Клиент отказался вставлять новую запись 
                await client.SendAsync(OkPacket.Instance, token);
                return;
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
            case PacketType.Ok:
                throw new UnexpectedPacketException(ackPacket, PacketType.AcknowledgeRequest);
        }

        var submitResponse = await _requestAcceptor.AcceptAsync(command, token);
        if (submitResponse.TryGetResponse(out var response))
        {
            await client.SendAsync(ResponsePacketMapper.MapResponse(response), token);
        }
        else
        {
            await client.SendAsync(new NotLeaderPacket(_clusterInfo.LeaderId?.Id), token);
        }
    }

    private async Task ProcessDequeueCommandAsync(TaskFluxClient client,
                                                  CommandRequestPacket packet,
                                                  CancellationToken token)
    {
        /*
         * Процесс чтения записи разбивается на 2 этапа:
         * 1. Забираем запись из самой очереди -> Отправляем клиенту
         * 2. Клиент подтверждает ее обработку -> Коммитим удаление
         *
         * В противном случае, пока клиент отсылал запрос и получал запись - отвалился.
         * В этом случае, запись будет потеряна.
         */
        var command = ( DequeueRecordCommand ) CommandMapper.Map(packet.Command);
        var submitResult = await _requestAcceptor.AcceptAsync(command, token);

        if (submitResult.TryGetResponse(out var response) is false)
        {
            // Перестали быть лидером пока запрос обрабатывался
            await client.SendAsync(new NotLeaderPacket(_clusterInfo.LeaderId?.Id), token);
            return;
        }

        await client.SendAsync(ResponsePacketMapper.MapResponse(response), token);

        if (response.Type != ResponseType.Dequeue)
        {
            // Если в ответе получили не Dequeue, то
            // единственный вариант - ошибка бизнес-логики (нарушение политики, очередь не существует и т.д.)
            return;
        }

        var dequeueResponse = ( DequeueResponse ) response;
        if (!dequeueResponse.Success)
        {
            // Если очередь была пуста - приступаем к обработке следующих команд
            return;
        }

        /*
         * На этом моменте мы знаем, что из какой-то очереди была прочитана запись,
         * но ее прочтение еще не закоммичено.
         * Теперь ждем:
         * - Ack - коммитим чтение
         * - Nack - возвращаем запись обратно
         */
        try
        {
            // Ожидаем либо Ack, либо Nack
            Packet request;
            using (var cts = CreateIdleTimeoutCts(token))
            {
                request = await client.ReceiveAsync(cts.Token);
            }

            Debug.Assert(Enum.IsDefined(request.Type), "Enum.IsDefined(request.Type)", "Неизвестный тип пакета {0}",
                request.Type);

            SubmitResponse<Response> submitResponse;
            switch (request.Type)
            {
                // Если команду все же нужно выполнить - коммитим выполнение и возвращаем ответ
                case PacketType.AcknowledgeRequest:
                    // Явно указываем, что нужно коммитить
                    dequeueResponse = dequeueResponse.WithDeltaProducing();
                    submitResponse =
                        await _requestAcceptor.AcceptAsync(new CommitDequeueCommand(dequeueResponse), token);
                    break;
                // Иначе возвращаем ее обратно
                case PacketType.NegativeAcknowledgementRequest:
                    // Коммитить результат не нужно
                    dequeueResponse = dequeueResponse.WithoutDeltaProducing();
                    submitResponse =
                        await _requestAcceptor.AcceptAsync(new ReturnRecordCommand(dequeueResponse), token);
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
                    throw new UnexpectedPacketException(request, PacketType.AcknowledgeRequest);
                default:
                    throw new ArgumentOutOfRangeException(nameof(request.Type), request.Type, "Неизвестный тип пакета");
            }

            if (submitResponse.HasValue)
            {
                // В любом случае (Ack/Nack) отвечаем OK
                await client.SendAsync(OkPacket.Instance, token);
            }
            else
            {
                // Если пока отрабатывали лидер кластера сменился
                await client.SendAsync(new NotLeaderPacket(_clusterInfo.LeaderId?.Id), token);
            }
        }
        catch (Exception)
        {
            await _requestAcceptor.AcceptAsync(new ReturnRecordCommand(dequeueResponse), token);
            throw;
        }
    }

    private CancellationTokenSource CreateIdleTimeoutCts(CancellationToken token)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(_options.CurrentValue.IdleTimeout);
        return cts;
    }
}