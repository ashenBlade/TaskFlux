namespace TaskFlux.Consensus;

/// <summary>
/// Объект, занимающийся принятием запросов от внешних клиентов
/// </summary>
/// <typeparam name="TCommand">Класс команды</typeparam>
/// <typeparam name="TResponse">Класс результата выполнения команды</typeparam>
public interface IConsensusModule<in TCommand, TResponse>
{
    /// <summary>
    /// Принять запрос на применение команды к приложению
    /// </summary>
    /// <param name="command">Команда, которую нужно выполнить</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Результат выполнения работы</returns>
    public SubmitResponse<TResponse> Handle(TCommand command, CancellationToken token = default);
}