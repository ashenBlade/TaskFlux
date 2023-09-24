using TaskFlux.Commands;
using TaskFlux.Network.Client.Exceptions;

namespace TaskFlux.Network.Client;

public interface ITaskFluxClient : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Отправить команду на узел для выполения
    /// </summary>
    /// <param name="command">Команда, которую нужно выполнить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат операции</returns>
    /// <exception cref="ArgumentNullException"><paramref name="command"/> - null</exception>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    /// <exception cref="TaskFluxException">Базовый тип исключений</exception>
    public Task<Result> SendAsync(Command command, CancellationToken token = default);
}