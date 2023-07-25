using System.Net;
using TaskFlux.Network.Requests;
using TaskFlux.Network.Requests.Serialization.Exceptions;

namespace TaskFlux.Network.Client;

public interface ITaskFluxClient
{
    /// <summary>
    /// Подключиться к узлу по переданному <paramref name="endPoint"/> 
    /// </summary>
    /// <param name="endPoint">Адрес удаленного узла</param>
    /// <param name="token">Токен отмены</param>
    /// <returns><see cref="ValueTask"/></returns>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    /// <exception cref="ArgumentNullException"><paramref name="endPoint"/> - <c>null</c></exception>
    public ValueTask ConnectAsync(EndPoint endPoint, CancellationToken token = default);
    
    /// <summary>
    /// Отключиться от удаленного узла
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns><see cref="ValueTask"/>, завершающийся при окончании закрытия соединения</returns>
    public ValueTask DisconnectAsync(CancellationToken token = default);
    
    /// <summary>
    /// Отправить на удаленный узел указанный пакет
    /// </summary>
    /// <param name="packet">Пакет, который необходимо отправить на другой узел</param>
    /// <param name="token">Токен отмены</param>
    /// <returns><see cref="ValueTask"/></returns>
    /// <exception cref="ArgumentNullException"> - <paramref name="packet"/> <c>null</c></exception>
    /// <exception cref="OperationCanceledException"> - токен был отменен</exception>
    /// <exception cref="IOException">Ошибка сети во время отправки сообщения</exception>
    public ValueTask SendAsync(Packet packet, CancellationToken token = default);
    
    /// <summary>
    /// Получить от удаленного узла пакет
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns><see cref="ValueTask{Packet}"/> который возвращает <see cref="Packet"/> при завершении операции</returns>
    /// <exception cref="OperationCanceledException"> токен был отменен</exception>
    /// <exception cref="IOException">Во время чтения возникла ошибка сети</exception>
    /// <exception cref="EndOfStreamException">Соединение было разорвано во время чтения из потока</exception>
    /// <exception cref="PacketDeserializationException">Произошла ошибка десериализации конкретного пакета</exception>
    public ValueTask<Packet> ReceiveAsync(CancellationToken token = default);
}