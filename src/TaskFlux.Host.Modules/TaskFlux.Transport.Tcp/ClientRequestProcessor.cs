using System.Diagnostics;
using System.Net.Sockets;
using Serilog;
using TaskFlux.Application;
using TaskFlux.Consensus;
using TaskFlux.Core.Commands;
using TaskFlux.Core.Commands.Dequeue;
using TaskFlux.Network;
using TaskFlux.Network.Authorization;
using TaskFlux.Network.Commands;
using TaskFlux.Network.Exceptions;
using TaskFlux.Network.Packets;
using TaskFlux.Transport.Tcp.Exceptions;
using TaskFlux.Transport.Tcp.Mapping;
using IApplicationLifetime = TaskFlux.Application.IApplicationLifetime;

namespace TaskFlux.Transport.Tcp;

internal class ClientRequestProcessor
{
    private readonly TcpClient _client;
    private readonly TcpAdapterOptions _options;
    private readonly IRequestAcceptor _requestAcceptor;
    private readonly IApplicationInfo _applicationInfo;
    private readonly IApplicationLifetime _lifetime;
    private readonly ILogger _logger;

    public ClientRequestProcessor(TcpClient client,
                                  IRequestAcceptor requestAcceptor,
                                  TcpAdapterOptions options,
                                  IApplicationInfo applicationInfo,
                                  IApplicationLifetime lifetime,
                                  ILogger logger)
    {
        _client = client;
        _requestAcceptor = requestAcceptor;
        _options = options;
        _logger = logger;
        _applicationInfo = applicationInfo;
        _lifetime = lifetime;
    }

    public async Task ProcessAsync(CancellationToken token)
    {
        _logger.Information("Начинаю обработку клиента");
        await using var stream = _client.GetStream();
        var client = new TaskFluxClient(stream);
        Metrics.TotalConnectedClients.Add(1);
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
        catch (UnknownQueuePolicyException uqpe)
        {
            _logger.Warning(uqpe, "От клиента получен неизвестный тип политики очереди");
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
            _lifetime.StopAbnormal();
        }
        finally
        {
            Metrics.TotalDisconnectedClients.Add(1);
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
                _logger.Information(
                    "От клиента получен неожиданный пакет. Ожидался пакет авторизации. Получен: {Packet}",
                    request.Type);
                return false;
            default:
                Debug.Assert(false, "false", "Получено неизвестное значение типа пакета: {0}", request.Type);
                throw new ArgumentOutOfRangeException(nameof(request.Type), ( int ) request.Type,
                    "Неизвестное значение типа пакета");
        }

        var authorizationType = authorizationRequest.AuthorizationMethod.AuthorizationMethodType;
        switch (authorizationType)
        {
            case AuthorizationMethodType.None:
                // Единственный пока метод авторизации
                break;
            default:
                Debug.Assert(false, "false", "Неизвестное значение метода авторизации: {0}", authorizationType);
                throw new ArgumentOutOfRangeException(nameof(authorizationType),
                    ( int ) authorizationType, "Неизвестное значение метода авторизации");
        }

        await client.SendAsync(AuthorizationResponsePacket.Ok, token);

        // 2. Настройка клиента
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
            case PacketType.NegativeAcknowledgementRequest:
            case PacketType.Ok:
                _logger.Warning("От клиента получен неожиданный пакет {PacketType}", request.Type);
                return false;
            default:
                Debug.Assert(false, "false", "Неизвестное значение типа пакета: {0}", request.Type);
                throw new ArgumentOutOfRangeException(nameof(request.Type), request.Type,
                    "Неизвестное значение типа пакета");
        }

        if (!IsVersionSupported(bootstrapRequest))
        {
            await client.SendAsync(BootstrapResponsePacket.Error("Версия клиента не поддерживается"), token);
            return false;
        }

        await client.SendAsync(BootstrapResponsePacket.Ok, token);
        return true;

        bool IsVersionSupported(BootstrapRequestPacket packet)
        {
            var serverVersion = _applicationInfo.ApplicationVersion;
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

            Metrics.TotalAcceptedRequests.Add(1);

            try
            {
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
                    case PacketType.NegativeAcknowledgementRequest:
                    case PacketType.Ok:
                        _logger.Warning("От клиента получен неожиданный тип пакета {PacketType}", request.Type);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(request.Type), ( int ) request.Type,
                            "Получен неизвестный тип пакета");
                }
            }
            finally
            {
                Metrics.TotalProcessedRequests.Add(1);
            }
        }
    }

    private async Task ProcessClusterMetadataRequestAsync(TaskFluxClient client,
                                                          CancellationToken token)
    {
        var response = new ClusterMetadataResponsePacket(_applicationInfo.Nodes, _applicationInfo.LeaderId?.Id,
            _applicationInfo.NodeId.Id);
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
                Debug.Assert(false, "Неизвестный тип команды", "Получен неизвестный тип NetworkCommand: {0}",
                    request.Command.Type);
                throw new ArgumentOutOfRangeException(nameof(request.Command.Type), request.Command.Type,
                    "Получен неизвестный тип команды");
        }

        Command command;
        try
        {
            command = CommandMapper.Map(request.Command);
        }
        catch (MappingException me)
        {
            await client.SendAsync(new ErrorResponsePacket(( int ) me.ErrorCode,
                    string.Empty /* На данном этапе все коды ясно говорят какая ошибка произошла - доп. сообщений не надо */),
                token);
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
            await client.SendAsync(new NotLeaderPacket(_applicationInfo.LeaderId?.Id ?? null), token);
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
        catch (MappingException me)
        {
            // Указанное пользователем название очереди невалидное
            await client.SendAsync(new ErrorResponsePacket(( int ) me.ErrorCode, string.Empty), token);
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
            await client.SendAsync(new NotLeaderPacket(_applicationInfo.LeaderId?.Id ?? null), token);
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
            await client.SendAsync(new NotLeaderPacket(_applicationInfo.LeaderId?.Id), token);
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

            SubmitResponse<Response> submitResponse;
            switch (request.Type)
            {
                // Если команду все же нужно выполнить - коммитим выполнение и возвращаем ответ
                case PacketType.AcknowledgeRequest:
                    // Явно указываем, что нужно коммитить
                    dequeueResponse.MakePersistent();
                    submitResponse =
                        await _requestAcceptor.AcceptAsync(new CommitDequeueCommand(dequeueResponse), token);
                    break;
                // Иначе возвращаем ее обратно
                case PacketType.NegativeAcknowledgementRequest:
                    // Коммитить результат не нужно
                    dequeueResponse.MakeNonPersistent();
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
                await client.SendAsync(new NotLeaderPacket(_applicationInfo.LeaderId?.Id), token);
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
        cts.CancelAfter(_options.IdleTimeout);
        return cts;
    }
}