using Consensus.Raft.Persistence;

namespace Consensus.Raft;

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

    /// <summary>
    /// Получить слепок состояния машины на текущий момент
    /// </summary>
    /// <returns>Слепок состояния машины</returns>
    public ISnapshot GetSnapshot();
}