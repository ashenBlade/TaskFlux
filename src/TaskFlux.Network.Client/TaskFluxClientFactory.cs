using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using TaskFlux.Commands.Serialization;
using TaskFlux.Network.Client.Exceptions;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Packets.Authorization;
using TaskFlux.Network.Packets.Packets;
using TaskFlux.Network.Packets.Serialization;

namespace TaskFlux.Network.Client;

public class TaskFluxClientFactory : ITaskFluxClientFactory
{
    private const int DefaultPort = 2602;

    internal readonly CommandSerializer CommandSerializer = CommandSerializer.Instance;
    internal readonly ResultSerializer ResultSerializer = ResultSerializer.Instance;

    /// <summary>
    /// Конечные точки узлов кластера.
    /// Индекс указывает на Id узла
    /// </summary>
    private readonly EndPoint[] Endpoints;

    /// <summary>
    /// Id текущего известного лидера кластера
    /// </summary>
    internal int? LeaderId;

    /// <summary>
    /// Создать новую фабрику клиентов с предустановленными адресами узлов кластера
    /// </summary>
    /// <param name="endpoints">Адреса узлов с индексами соответствующими Id узла</param>
    public TaskFluxClientFactory(EndPoint[] endpoints)
    {
        Endpoints = endpoints;
    }

    private TaskFluxClientFactory(EndPoint[] endpoints, int? leaderId)
    {
        Endpoints = endpoints;
        LeaderId = leaderId;
    }

    public async Task<ITaskFluxClient> CreateClientAsync(CancellationToken token = default)
    {
        var connection = await EstablishConnectionAsync(token);
        return new TaskFluxClient(connection, this);
    }

    /// <summary>
    /// Установить новое соединение с узлом лидером
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Клиент соединения и соответствующий ему сокет</returns>
    internal async Task<(TaskFluxPacketClient Client, Socket Socket)> EstablishConnectionAsync(
        CancellationToken token = default)
    {
        // 1. Подключаемся к узлу: либо известному лидеру, либо любому (если не известен лидер - все равно обнаружим лидера потом)
        var socket = await ConnectAsync(token);

        try
        {
            var client = new TaskFluxPacketClient(ArrayPool<byte>.Shared, new NetworkStream(socket));

            // 2. Выполняем процесс авторизации
            await client.SendAsync(new AuthorizationRequestPacket(NoneAuthorizationMethod.Instance), token);
            var response = await client.ReceiveAsync(token);
            switch (response.Type)
            {
                case PacketType.AuthorizationResponse:
                    break;
                case PacketType.ErrorResponse:
                    throw new ServerErrorException(( ( ErrorResponsePacket ) response ).Message);

                case PacketType.CommandRequest:
                case PacketType.CommandResponse:
                case PacketType.NotLeader:
                case PacketType.AuthorizationRequest:
                case PacketType.BootstrapRequest:
                case PacketType.BootstrapResponse:
                case PacketType.ClusterMetadataRequest:
                case PacketType.ClusterMetadataResponse:
                    throw new UnexpectedResponseException(response.Type, PacketType.AuthorizationResponse);
                default:
                    Debug.Assert(false, "Тип PacketType в ответе сервера неизвестен",
                        $"Полученный маркер {( int ) response.Type}. Тело: {response}");
                    throw new UnexpectedResponseException(response.Type, PacketType.AuthorizationResponse);
            }

            var authorizationResponse = ( AuthorizationResponsePacket ) response;
            if (authorizationResponse.Success is false)
            {
                throw new AuthorizationException(authorizationResponse.ErrorReason);
            }

            // 3. Выполняем процесс настройки
            await client.SendAsync(new BootstrapRequestPacket(Constants.ServerVersion.Major,
                Constants.ServerVersion.Minor, Constants.ServerVersion.Build), token);
            response = await client.ReceiveAsync(token);
            switch (response.Type)
            {
                case PacketType.BootstrapResponse:
                    break;
                case PacketType.ErrorResponse:
                    throw new ServerErrorException(( ( ErrorResponsePacket ) response ).Message);

                case PacketType.CommandRequest:
                case PacketType.CommandResponse:
                case PacketType.NotLeader:
                case PacketType.AuthorizationRequest:
                case PacketType.AuthorizationResponse:
                case PacketType.BootstrapRequest:
                case PacketType.ClusterMetadataRequest:
                case PacketType.ClusterMetadataResponse:
                    throw new UnexpectedResponseException(response.Type, PacketType.BootstrapResponse);
                default:
                    Debug.Assert(false, "От сервера получен неожиданный пакет",
                        "От сервера получен пакет {0}, когда ожидался {1}. Содержимое пакета: {2}", response.Type,
                        PacketType.BootstrapResponse, response);
                    throw new UnexpectedResponseException(response.Type, PacketType.BootstrapResponse);
            }

            var bootstrapResponse = ( BootstrapResponsePacket ) response;
            if (bootstrapResponse.Success is false)
            {
                throw new BootstrapException(bootstrapResponse.Reason);
            }

            return ( client, socket );
        }
        catch (Exception)
        {
            socket.Close();
            throw;
        }
    }

    private async Task<Socket> ConnectAsync(CancellationToken token)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            if (LeaderId is { } id)
            {
                // Если можем установить соединение с текущим лидером, то делаем это сразу
                var leaderEndpoint = Endpoints[id];
                await socket.ConnectAsync(leaderEndpoint, token);
                return socket;
            }

            // Если лидер неизвестен - пытаемся подключиться к каждому узлу постепенно 
            foreach (var endPoint in Endpoints)
            {
                try
                {
                    await socket.ConnectAsync(endPoint, token);
                    return socket;
                }
                catch (SocketException)
                {
                }
            }

            throw new ConnectionException("Не удалось подключиться ни к одному из указанных узлов");
        }
        catch (Exception)
        {
            socket.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Создать новую фабрику для кластера.
    /// Во время вызова, последовательно выполняются запросы к каждому хосту, указанному в <paramref name="bootstrapServers"/>.
    /// Из каждого получаются метаданные кластера.
    /// </summary>
    /// <param name="bootstrapServers">Серверы, из которых нужно получить метаданные кластера</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Инициализированная фабрика для клиентов TaskFlux</returns>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    /// <exception cref="ArgumentException">Какая-то строка представляет невалидный адрес хоста <br/></exception>
    /// <exception cref="ArgumentNullException"><paramref name="bootstrapServers"/> или одна из строк - null</exception>
    /// <exception cref="BootstrapException">Ошибка во время начальной настройки клиента и сервера</exception>
    /// <exception cref="AuthorizationException">Ошибка во время авторизации на сервере</exception>
    /// <exception cref="ConnectionException">Ни к одному из переданных адресов не удалось подключиться</exception>
    /// <exception cref="TaskFluxException">Базовый тип исключений, связанный с логикой работы</exception>
    public static async Task<TaskFluxClientFactory> ConnectAsync(string[] bootstrapServers,
                                                                 CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(bootstrapServers);
        foreach (var address in bootstrapServers)
        {
            ArgumentNullException.ThrowIfNull(address);
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                // 1. Подключаемся
                var endpoint = EndPointHelpers.Parse(address, DefaultPort);
                try
                {
                    await socket.ConnectAsync(endpoint, token);
                }
                catch (SocketException)
                {
                    socket.Dispose();
                    continue;
                }

                // 2. Авторизуемся
                var client = new TaskFluxPacketClient(ArrayPool<byte>.Shared, new NetworkStream(socket));
                await client.SendAsync(new AuthorizationRequestPacket(NoneAuthorizationMethod.Instance), token);
                var response = await client.ReceiveAsync(token);
                switch (response.Type)
                {
                    case PacketType.AuthorizationResponse:
                        break;
                    case PacketType.ErrorResponse:
                        continue;
                    case PacketType.CommandRequest:
                    case PacketType.CommandResponse:
                    case PacketType.NotLeader:
                    case PacketType.AuthorizationRequest:
                    case PacketType.BootstrapRequest:
                    case PacketType.BootstrapResponse:
                    default:
                        Debug.Assert(false, $"Получен неожиданный пакет во время авторизации",
                            "Получен {0} пакет. Ожидался {1}. Тело: {2}", response.Type,
                            PacketType.AuthorizationResponse, response);
                        throw new UnexpectedResponseException(response.Type, PacketType.AuthorizationResponse);
                }

                var authorizationResponse = ( AuthorizationResponsePacket ) response;
                if (!authorizationResponse.Success)
                {
                    throw new AuthorizationException(authorizationResponse.ErrorReason);
                }

                // 3. Получаем метаданные кластера
                await client.SendAsync(new BootstrapRequestPacket(Constants.ServerVersion.Major,
                    Constants.ServerVersion.Minor, Constants.ServerVersion.Build), token);

                response = await client.ReceiveAsync(token);
                switch (response.Type)
                {
                    case PacketType.BootstrapResponse:
                        break;
                    case PacketType.ErrorResponse:
                        continue;

                    case PacketType.CommandRequest:
                    case PacketType.CommandResponse:
                    case PacketType.NotLeader:
                    case PacketType.AuthorizationRequest:
                    case PacketType.AuthorizationResponse:
                    case PacketType.BootstrapRequest:
                    default:
                        Debug.Assert(false, $"Получен неожиданный пакет данных во время настройки",
                            "Получен {0} пакет. Ожидался {1}. Тело: {2}", response.Type, PacketType.BootstrapResponse,
                            response);
                        throw new UnexpectedResponseException(response.Type, PacketType.BootstrapResponse);
                }

                var bootstrapResponse = ( BootstrapResponsePacket ) response;
                if (!bootstrapResponse.Success)
                {
                    throw new BootstrapException(bootstrapResponse.Reason);
                }

                // 3. Получаем метаданные
                await client.SendAsync(new ClusterMetadataRequestPacket(), token);
                response = await client.ReceiveAsync(token);

                switch (response.Type)
                {
                    case PacketType.ClusterMetadataResponse:
                        break;
                    case PacketType.ErrorResponse:
                        continue;

                    case PacketType.CommandRequest:
                    case PacketType.CommandResponse:
                    case PacketType.NotLeader:
                    case PacketType.AuthorizationRequest:
                    case PacketType.AuthorizationResponse:
                    case PacketType.BootstrapRequest:
                    case PacketType.BootstrapResponse:
                    case PacketType.ClusterMetadataRequest:
                    default:
                        Debug.Assert(false,
                            "От сервера получен неожиданный пакет во время получения метаданных кластера",
                            "От сервера получен {0} пакет. Ожидался {1}. Тело: {2}", response.Type,
                            PacketType.ClusterMetadataResponse, response);
                        throw new UnexpectedResponseException(response.Type, PacketType.ClusterMetadataResponse);
                }

                var clusterMetadataResponse = ( ClusterMetadataResponsePacket ) response;
                var factory =
                    new TaskFluxClientFactory(clusterMetadataResponse.EndPoints, clusterMetadataResponse.LeaderId);
                socket.Close();
                return factory;
            }
            catch (Exception)
            {
                socket.Close();
                throw;
            }
        }

        throw new ConnectionException("Ни к однму из переданных узлов не удалось подключиться");
    }
}