using TaskFlux.Network;
using TaskFlux.Network.Exceptions;

namespace TaskFlux.Host.Modules.SocketRequest;

/// <summary>
/// Удобный интерфейс для отправки пакетов клиентам
/// </summary>
public interface ITaskFluxClient
{
    /// <summary>
    /// Отправить указанный пакет
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> отменен</exception>
    /// <exception cref="EndOfStreamException">Соединение было закрыто</exception>
    public Task SendAsync(Packet packet, CancellationToken token);

    /// <summary>
    /// Получить от клиента пакет
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный пакет</returns>
    /// <exception cref="EndOfStreamException">Во время чтения клиент закрыл соединение</exception>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    /// <exception cref="UnknownPacketException">Клиент отправил неизвестный тип пакета (маркер неизвестен)</exception>
    /// <exception cref="UnknownCommandTypeException">Клиент отправил неизвестную команду</exception>
    /// <exception cref="UnknownResponseTypeException">Клиент отправил неизвестный ответ</exception>
    /// <exception cref="UnknownAuthorizationMethodException">Клиент отправил неизвестный метод авторизации</exception>
    public Task<Packet> ReceiveAsync(CancellationToken token);
}