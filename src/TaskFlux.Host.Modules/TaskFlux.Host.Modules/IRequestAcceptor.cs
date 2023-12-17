using Consensus.Core.Submit;
using TaskFlux.Commands;

namespace TaskFlux.Host.Modules;

public interface IRequestAcceptor
{
    /// <summary>
    /// Обработать запрос от пользователя
    /// </summary>
    /// <param name="command">Команда, которую нужно выполнить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат выполнения запроса</returns>
    public Task<SubmitResponse<Response>> AcceptAsync(Command command, CancellationToken token = default);
}