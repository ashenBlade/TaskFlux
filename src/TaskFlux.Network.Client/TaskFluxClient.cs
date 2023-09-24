using System.Diagnostics;
using System.Net.Sockets;
using TaskFlux.Commands;
using TaskFlux.Network.Client.Exceptions;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Packets.Packets;
using TaskFlux.Network.Packets.Serialization;
using TaskFlux.Network.Packets.Serialization.Exceptions;

namespace TaskFlux.Network.Client;

internal class TaskFluxClient : ITaskFluxClient
{
    /// <summary>
    /// Объект соединения с узлом.
    /// Если null, то соединение не установлено/сброшено
    /// </summary>
    private (TaskFluxPacketClient Client, System.Net.Sockets.Socket Socket)? _connection;

    /// <summary>
    /// Фабрика, которой принадлежит этот клиент.
    /// Нужна для установления соединений с кластером
    /// </summary>
    private readonly TaskFluxClientFactory _factory;

    private bool _disposed;


    public TaskFluxClient((TaskFluxPacketClient, Socket) connection, TaskFluxClientFactory factory)
    {
        _connection = connection;
        _factory = factory;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(TaskFluxClient));
        }
    }

    public async Task<Result> SendAsync(Command command, CancellationToken token = default)
    {
        ThrowIfDisposed();
        token.ThrowIfCancellationRequested();

        TaskFluxPacketClient client;
        Socket socket;
        if (_connection == null)
        {
            var connection = await _factory.EstablishConnectionAsync(token);
            _connection = connection;
            ( client, socket ) = connection;
        }
        else
        {
            ( client, socket ) = _connection.Value;
        }

        token.ThrowIfCancellationRequested();
        var commandPacket = new CommandRequestPacket(_factory.CommandSerializer.Serialize(command));
        while (!token.IsCancellationRequested)
        {
            try
            {
                var result = await SendCommandCoreAsync(commandPacket, client, token);
                if (result is not null)
                {
                    return result;
                }
            }
            catch (EndOfStreamException)
            {
                /*
                 * Соединение было разорвано
                 */
            }
            catch (UnknownPacketException upe)
            {
                socket.Close();
                _connection = null;
                throw new InvalidResponseException("От сервера получен неожиданный пакет", upe);
            }
            catch (PacketDeserializationException pde)
            {
                socket.Close();
                _connection = null;
                throw new InvalidResponseException("Ошибка во время десериализации ответа от сервера", pde);
            }
            catch (ServerErrorException)
            {
                socket.Close();
                _connection = null;
                throw;
            }
            catch (UnexpectedResponseException)
            {
                socket.Close();
                _connection = null;
                throw;
            }


            // Пересоздаем подключение
            socket.Close();
            _connection = null;

            token.ThrowIfCancellationRequested();
            var connection = await _factory.EstablishConnectionAsync(token);
            _connection = connection;
            ( client, socket ) = connection;
        }

        // Единственный вариант развития событий
        throw new OperationCanceledException(token);
    }


    /// <summary>
    /// Общая логика отправки команды на узел
    /// </summary>
    /// <param name="packet">Пакет команды, которую нужно отправить</param>
    /// <param name="client">Клиент, который нужно использовать</param>
    /// <param name="token">Токен отмены</param>
    /// <returns><see cref="Result"/> - команда выполнена успешно, <c>null</c> - нужно сделать повторную попытку (например, найден новый лидер)</returns>
    private async Task<Result?> SendCommandCoreAsync(CommandRequestPacket packet,
                                                     TaskFluxPacketClient client,
                                                     CancellationToken token = default)
    {
        await client.SendAsync(packet, token);
        var response = await client.ReceiveAsync(token);
        switch (response.Type)
        {
            case PacketType.CommandResponse:
                break;
            case PacketType.ErrorResponse:
                throw new ServerErrorException(( ( ErrorResponsePacket ) response ).Message);

            case PacketType.NotLeader:
                // Обновляем лидера, делаем повторную попытку
                var notLeaderResponse = ( NotLeaderPacket ) response;
                _factory.LeaderId = notLeaderResponse.LeaderId;
                return null;
            case PacketType.CommandRequest:
            case PacketType.AuthorizationRequest:
            case PacketType.AuthorizationResponse:
            case PacketType.BootstrapRequest:
            case PacketType.BootstrapResponse:
            case PacketType.ClusterMetadataRequest:
            case PacketType.ClusterMetadataResponse:
                Debug.Assert(false, "От сервера получен неожиданный пакет", "Ожидался {0}. Получен: {1}. Тело: {2}",
                    PacketType.CommandResponse, response.Type, response);
                throw new UnexpectedResponseException(response.Type, PacketType.CommandResponse);
            default:
                Debug.Assert(false, "От сервера получен неожиданный пакет", "Ожидался {0}. Получен: {1}. Тело: {2}",
                    PacketType.CommandResponse, response.Type, response);
                throw new UnexpectedResponseException(response.Type, PacketType.CommandResponse);
        }

        var commandResponse = ( CommandResponsePacket ) response;

        return _factory.ResultSerializer.Deserialize(commandResponse.Payload);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_connection is {Socket: var socket})
        {
            socket.Close();
            socket.Dispose();
        }
    }
}