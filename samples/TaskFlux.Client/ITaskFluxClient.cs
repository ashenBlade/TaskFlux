using TaskFlux.Client.Exceptions;
using TaskFlux.Models;
using TaskFlux.Network.Responses;

namespace TaskFlux.Client;

public interface ITaskFluxClient : IDisposable, IAsyncDisposable
{
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
    Task EnqueueAsync(QueueName queue, long key, byte[] message, CancellationToken token = default);

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
    Task<(long Key, byte[] Message)> DequeueAsync(QueueName queue, CancellationToken token = default);

    /// <summary>
    /// Получить информацию о всех очередях 
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Информация о всех очередях</returns>
    /// <exception cref="UnexpectedPacketException">Сервер вернул неожиданный пакет</exception>
    /// <exception cref="UnexpectedResponseException">Сервер вернул неожиданный ответ</exception>
    /// <exception cref="NotLeaderException">Сервер перестал быть лидером кластера</exception>
    /// <exception cref="ErrorResponseException">Сервер вернул ошибку</exception>
    Task<List<ITaskQueueInfo>> GetAllQueuesInfoAsync(CancellationToken token);

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
    Task<int> GetQueueLengthAsync(QueueName queueName, CancellationToken token);

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
    Task CreateQueueAsync(QueueName queueName, CreateQueueOptions options, CancellationToken token);

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
    Task DeleteQueueAsync(QueueName queueName, CancellationToken token);
}