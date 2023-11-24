using Consensus.Raft.Commands.Submit;

namespace Consensus.Core;

/// <summary>
/// Объект, занимающийся принятием запросов от внешних клиентов
/// </summary>
/// <typeparam name="TCommand">Класс команды</typeparam>
/// <typeparam name="TResponse">Класс результата выполнения команды</typeparam>
public interface IConsensusModule<TCommand, TResponse>
{
    /// <summary>
    /// Принять запрос на применение команды к приложению
    /// </summary>
    /// <param name="request">Объект самого запроса</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат выполнения работы</returns>
    public SubmitResponse<TResponse> Handle(SubmitRequest<TCommand> request, CancellationToken token = default);
}