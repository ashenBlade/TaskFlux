using System.Diagnostics;
using System.Net.Sockets;
using TaskFlux.Client.Exceptions;
using TaskFlux.Core;
using TaskFlux.Network;
using TaskFlux.Network.Commands;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Responses;

namespace TaskFlux.Client;

internal class TaskFluxClient : ITaskFluxClient
{
    /// <summary>
    /// Поток для взаимодействия с конкретным узлом.
    /// При разрыве соединения, поток будет обновлен
    /// </summary>
    /// <remarks>
    /// Если поток - <c>null</c>, то был вызван <see cref="Dispose"/>
    /// </remarks>
    private NetworkStream? _stream;

    internal TaskFluxClient(NetworkStream stream)
    {
        _stream = stream;
    }

    private void CheckDisposed()
    {
        if (_stream is null)
        {
            throw new ObjectDisposedException(nameof(TaskFluxClient));
        }
    }

    /// <summary>
    /// Добавить новую запись в очередь <paramref name="queue"/>
    /// </summary>
    /// <param name="queue">Название очереди</param>
    /// <param name="key">Ключ записи, приоритет</param>
    /// <param name="message">Сообщение</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="PolicyViolationException">При вставке была нарушена политика</exception>
    /// <exception cref="QueueNotExistException">Указанной очереди не существует</exception>
    /// <exception cref="UnexpectedPacketException">От сервера пришел неожиданный пакет</exception>
    /// <exception cref="UnexpectedResponseException">Сервер отправио неожиданный ответ</exception>
    /// <exception cref="NotLeaderException">Узел, с которым велось взаимодействие, перестал быть лидером</exception>
    /// <exception cref="ErrorResponseException">Сервер ответил ошибкой</exception>
    public async Task EnqueueAsync(QueueName queue, long key, byte[] message, CancellationToken token = default)
    {
        CheckDisposed();
        Debug.Assert(_stream is not null, "_stream is not null",
            "Поток соединения null, но проверка CheckDisposed прошла без исключения");

        // 1. Отправляем запрос
        var enqueuePacket = new CommandRequestPacket(new EnqueueNetworkCommand(queue, key, message));
        await enqueuePacket.SerializeAsync(_stream, token);
        // 2. Ждем Ok
        var enqueueOk = await Packet.DeserializeAsync(_stream, token);
        ValidateOkResponse(enqueueOk);

        // 3. Отправляем Ack
        var ack = new AcknowledgeRequestPacket();
        await ack.SerializeAsync(_stream, token);

        // 4. Ждем Ok
        var ackOk = await Packet.DeserializeAsync(_stream, token);
        ValidateOkResponse(ackOk);
    }

    /// <summary>
    /// Прочитать из очереди <paramref name="queue"/> запись
    /// </summary>
    /// <param name="queue">Название очереди, из которой нужно прочитать запись</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Прочитанные ключ и сообщение</returns>
    /// <exception cref="QueueEmptyException">Очередь была пуста</exception>
    /// <exception cref="QueueNotExistException">Указанной очереди не существует</exception>
    /// <exception cref="UnexpectedPacketException">От сервера пришел неожиданный пакет</exception>
    /// <exception cref="UnexpectedResponseException">Сервер отправио неожиданный ответ</exception>
    /// <exception cref="NotLeaderException">Узел, с которым велось взаимодействие, перестал быть лидером</exception>
    /// <exception cref="ErrorResponseException">Сервер ответил ошибкой</exception>
    public async Task<(long Key, byte[] Message)> DequeueAsync(QueueName queue, CancellationToken token = default)
    {
        CheckDisposed();
        Debug.Assert(_stream is not null, "_stream is not null",
            "Поток соединения null, но проверка CheckDisposed прошла без исключения");

        // 1. Отправляем DequeueRequest
        var dequeueRequest = new CommandRequestPacket(new DequeueNetworkCommand(queue));
        await dequeueRequest.SerializeAsync(_stream, token);

        // 2. Получаем DequeueResponse
        var response = await Packet.DeserializeAsync(_stream, token);
        var (key, message) = ExtractDequeueResponse(response);

        // 3. Отправляем Ack
        await new AcknowledgeRequestPacket().SerializeAsync(_stream, token);

        // 4. Получаем Ok
        var ok = await Packet.DeserializeAsync(_stream, token);
        ValidateOkResponse(ok);

        return ( key, message );
    }

    /// <summary>
    /// Получить информацию о всех очередях 
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Информация о всех очередях</returns>
    /// <exception cref="UnexpectedPacketException">Сервер вернул неожиданный пакет</exception>
    /// <exception cref="UnexpectedResponseException">Сервер вернул неожиданный ответ</exception>
    /// <exception cref="NotLeaderException">Сервер перестал быть лидером кластера</exception>
    /// <exception cref="ErrorResponseException">Сервер вернул ошибку</exception>
    public async Task<List<ITaskQueueInfo>> GetAllQueuesInfoAsync(CancellationToken token)
    {
        CheckDisposed();
        Debug.Assert(_stream is not null, "_stream is not null",
            "Поток соединения null, но проверка CheckDisposed прошла без исключения");

        var requestPacket = new CommandRequestPacket(new ListQueuesNetworkCommand());
        await requestPacket.SerializeAsync(_stream, token);

        var response = await Packet.DeserializeAsync(_stream, token);
        Helpers.CheckNotErrorResponse(response);
        if (response.Type != PacketType.CommandResponse)
        {
            throw new UnexpectedPacketException(PacketType.CommandResponse, response.Type);
        }

        var commandResponse = ( CommandResponsePacket ) response;
        Helpers.CheckNotErrorResponse(commandResponse);
        if (commandResponse.Response.Type != NetworkResponseType.ListQueues)
        {
            throw new UnexpectedResponseException(NetworkResponseType.ListQueues, commandResponse.Response.Type);
        }

        var listQueues = ( ListQueuesNetworkResponse ) commandResponse.Response;
        return listQueues.Queues.ToList();
    }

    /// <summary>
    /// Получить размер указанной очереди
    /// </summary>
    /// <param name="queueName">Название очереди</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Размер указанной очереди</returns>
    /// <exception cref="UnexpectedPacketException">Сервер вернул неожиданный пакет</exception>
    /// <exception cref="UnexpectedResponseException">Сервер вернул неожиданный ответ</exception>
    /// <exception cref="NotLeaderException">Сервер перестал быть лидером кластера</exception>
    /// <exception cref="QueueNotExistException">Указанная очередь не существует</exception>
    /// <exception cref="ErrorResponseException">Сервер вернул ошибку</exception>
    public async Task<int> GetQueueLengthAsync(QueueName queueName, CancellationToken token)
    {
        CheckDisposed();
        Debug.Assert(_stream is not null, "_stream is not null",
            "Поток соединения null, но проверка CheckDisposed прошла без исключения");

        await new CommandRequestPacket(new CountNetworkCommand(queueName)).SerializeAsync(_stream, token);
        var response = await Packet.DeserializeAsync(_stream, token);
        Helpers.CheckNotErrorResponse(response);
        if (response.Type != PacketType.CommandResponse)
        {
            throw new UnexpectedPacketException(PacketType.CommandResponse, response.Type);
        }

        var commandResponse = ( CommandResponsePacket ) response;
        if (commandResponse.Response.Type != NetworkResponseType.Count)
        {
            throw new UnexpectedResponseException(NetworkResponseType.Count, commandResponse.Response.Type);
        }

        var countResponse = ( CountNetworkResponse ) commandResponse.Response;
        return countResponse.Count;
    }


    private static void ValidateOkResponse(Packet packet)
    {
        Helpers.CheckNotErrorResponse(packet);
        if (packet.Type != PacketType.Ok)
        {
            throw new UnexpectedPacketException(PacketType.Ok, packet.Type);
        }
    }

    private static (long Key, byte[] Message) ExtractDequeueResponse(Packet response)
    {
        Helpers.CheckNotErrorResponse(response);

        if (response.Type == PacketType.CommandResponse)
        {
            var commandResponse = ( CommandResponsePacket ) response;
            var networkResponse = commandResponse.Response;
            if (networkResponse.Type == NetworkResponseType.Dequeue)
            {
                var dequeueResponse = ( DequeueNetworkResponse ) networkResponse;
                if (dequeueResponse.TryGetResponse(out var key, out var message))
                {
                    return ( key, message );
                }

                throw new QueueEmptyException();
            }

            throw new UnexpectedResponseException(NetworkResponseType.Dequeue, networkResponse.Type);
        }

        throw new UnexpectedPacketException(PacketType.CommandResponse, response.Type);
    }

    /// <summary>
    /// Создать новую очередь с указанными параметрами
    /// </summary>
    /// <param name="queueName">Название очереди, которую нужно создать</param>
    /// <param name="options">Параметры создаваемой очереди</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="UnexpectedPacketException">Сервер вернул неожиданный пакет</exception>
    /// <exception cref="UnexpectedResponseException">Сервер вернул неожиданный ответ</exception>
    /// <exception cref="QueueAlreadyExistsException">Указанная очередь уже существует</exception>
    /// <exception cref="ErrorResponseException">Сервер вернул ошибку</exception>
    /// <exception cref="NotLeaderException">Сервер перестал быть лидером при отправке запроса</exception>
    public async Task CreateQueueAsync(QueueName queueName, CreateQueueOptions options, CancellationToken token)
    {
        CheckDisposed();
        Debug.Assert(_stream is not null, "_stream is not null",
            "Поток соединения null, но проверка CheckDisposed прошла без исключения");

        ArgumentNullException.ThrowIfNull(options);
        await new CommandRequestPacket(new CreateQueueNetworkCommand(queueName, options.ImplementationCode,
            options.MaxQueueSize,
            options.MaxMessageSize, options.PriorityRange)).SerializeAsync(_stream, token);
        var response = await Packet.DeserializeAsync(_stream, token);
        Helpers.CheckNotErrorResponse(response);
        if (response.Type != PacketType.Ok)
        {
            throw new UnexpectedPacketException(PacketType.Ok, response.Type);
        }
    }

    /// <summary>
    /// Удалить указанную очередь
    /// </summary>
    /// <param name="queueName">Название очереди, которую нужно удалить</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="UnexpectedPacketException">Сервер вернул неожиданный пакет</exception>
    /// <exception cref="UnexpectedResponseException">Сервер вернул неожиданный ответ</exception>
    /// <exception cref="QueueNotExistException">Указанная очередь не существует</exception>
    /// <exception cref="ErrorResponseException">Сервер вернул ошибку</exception>
    /// <exception cref="NotLeaderException">Сервер перестал быть лидером при отправке запроса</exception>
    public async Task DeleteQueueAsync(QueueName queueName, CancellationToken token)
    {
        CheckDisposed();
        Debug.Assert(_stream is not null, "_stream is not null",
            "Поток соединения null, но проверка CheckDisposed прошла без исключения");

        await new CommandRequestPacket(new DeleteQueueNetworkCommand(queueName)).SerializeAsync(_stream, token);
        var response = await Packet.DeserializeAsync(_stream, token);
        Helpers.CheckNotErrorResponse(response);
    }

    public void Dispose()
    {
        if (_stream is not null)
        {
            _stream.Dispose();
            _stream = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_stream != null)
        {
            await _stream.DisposeAsync();
            _stream = null;
        }
    }
}