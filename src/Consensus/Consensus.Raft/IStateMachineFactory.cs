using Consensus.Raft.Persistence;

namespace Consensus.Raft;

public interface IStateMachineFactory<in TCommand, out TResponse>
{
    /// <summary>
    /// Создать новую машину состояний.
    /// Используется при восстановлении состояния из снапшота
    /// </summary>
    /// <returns>Новая машина состояний</returns>
    public IStateMachine<TCommand, TResponse> CreateEmpty();
    
    /// <summary>
    /// Восстановить машину состояний из переданного снапшота
    /// </summary>
    /// <param name="snapshot">Объект снапшота машины состояний</param>
    /// <returns>Восстановленная машина состояний</returns>
    /// <exception cref="InvalidDataException">Переданный снапшот содержит невалидные данные</exception>
    public IStateMachine<TCommand, TResponse> Restore(ISnapshot snapshot);
}