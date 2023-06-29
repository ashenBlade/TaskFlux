namespace Raft.Network;

public interface INodeConnection
{
    /// <summary>
    /// Отправить на другой узел пакет
    /// </summary>
    /// <param name="packet">Пакет, который нужно отправить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns><c>true</c> - пакет отправлен успешно, <c>false</c> - не удалось отправить пакет</returns>
    public ValueTask<bool> SendAsync(IPacket packet, CancellationToken token = default);
    
    /// <summary>
    /// Получить от узла пакет данных
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Полученный пакет или <c>null</c> если возникла ошибка сети</returns>
    public ValueTask<IPacket?> ReceiveAsync(CancellationToken token = default);
}