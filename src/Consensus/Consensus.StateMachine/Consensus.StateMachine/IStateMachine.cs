namespace Consensus.StateMachine;

public interface IStateMachine<in TCommand, out TResponse>
{
    /// <summary>
    /// Применить команду к машине состояний и получить результат операции
    /// </summary>
    /// <param name="command">Команда, которую нужно выполнить</param>
    /// <returns>Результат операции</returns>
    public TResponse Apply(TCommand command);

    /// <summary>
    /// Применить команду к машине состояний без получения результата.
    /// Используется при репликации команд от лидера
    /// </summary>
    /// <param name="command">Команда, которую нужно выполнить</param>
    public void ApplyNoResponse(TCommand command);
}