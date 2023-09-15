using Consensus.Raft.Persistence;

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
    /// Применить команду к приложению без получения результата
    /// </summary>
    /// <param name="command">Команда, которую нужно выполнить</param>
    /// <remarks>
    /// Используется, когда ответ не отправляется клиенту. Например, репликация или восстановление
    /// </remarks>
    public void ApplyNoResponse(TCommand command);

    /// <summary>
    /// Получить слепок приложения на текущий момент
    /// </summary>
    /// <returns>Слепок приложения</returns>
    public ISnapshot GetSnapshot();
}