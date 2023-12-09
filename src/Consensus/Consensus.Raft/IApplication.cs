using Consensus.Core;

namespace Consensus.Raft;

public interface IApplication<in TCommand, out TResponse>
{
    /// <summary>
    /// Применить команду к приложению и получить результат операции
    /// </summary>
    /// <param name="command">Команда, которую нужно выполнить</param>
    /// <returns>Результат операции</returns>
    public TResponse Apply(TCommand command);

    /// <summary>
    /// Получить слепок приложения на текущий момент
    /// </summary>
    /// <returns>Слепок приложения</returns>
    public ISnapshot GetSnapshot();
}