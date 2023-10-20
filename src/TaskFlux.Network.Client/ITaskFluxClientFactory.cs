using TaskFlux.Network.Client.Exceptions;

namespace TaskFlux.Network.Client;

public interface ITaskFluxClientFactory
{
    /// <summary>
    /// Создать нового клиента для TaskFlux узла
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns>Новый клиент</returns>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    /// <exception cref="TaskFluxException">Базовый тип исключения</exception>
    public Task<ITaskFluxClient> CreateClientAsync(CancellationToken token = default);
}