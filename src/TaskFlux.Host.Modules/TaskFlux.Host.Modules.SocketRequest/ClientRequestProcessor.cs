using System.ComponentModel;
using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using Serilog;
using TaskFlux.Core;
using TaskFlux.Network.Client;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Packets.Authorization;
using TaskFlux.Network.Packets.Exceptions;
using TaskFlux.Network.Packets.Packets;

namespace TaskFlux.Host.Modules.SocketRequest;

internal class ClientRequestProcessor
{
    private readonly TcpClient _client;
    private readonly IOptionsMonitor<SocketRequestModuleOptions> _options;
    private readonly IApplicationInfo _applicationInfo;
    private readonly IRequestAcceptor _requestAcceptor;
    private IClusterInfo ClusterInfo { get; }
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
        ClusterInfo = clusterInfo;
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
        catch (EndOfStreamException)
        {
            _logger.Information("Клиент закрыл соединение");
            // Клиент разорвал соединение
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
            var serverVersion = Constants.ServerVersion;
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
        throw new NotImplementedException();
    }
}