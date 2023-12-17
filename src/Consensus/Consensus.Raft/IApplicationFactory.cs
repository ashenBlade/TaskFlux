using Consensus.Core;

namespace Consensus.Raft;

public interface IApplicationFactory<in TCommand, out TResponse>
{
    /// <summary>
    /// Восстановить состояние из переданных данных предыдущего состояния: снапшота и дельт из лога
    /// </summary>
    /// <param name="snapshot">Снапшот с предыдущим состоянием приложения</param>
    /// <param name="deltas">Дельты команд приложения</param>
    /// <remarks>
    /// <paramref name="snapshot"/> и <paramref name="deltas"/> могут быть пусты (или null).
    /// Это означает, что данные не были сохранены и нужно создать новое приложение
    /// </remarks>
    public IApplication<TCommand, TResponse> Restore(ISnapshot? snapshot, IEnumerable<byte[]> deltas);

    /// <summary>
    /// Создать новый снапшот, используя переданный старый (если был) и дельты изменений
    /// </summary>
    /// <param name="previousState">Предыдущее состояние, если было</param>
    /// <param name="deltas">Сериализованные дельты изменений</param>
    /// <returns>Новый снапшот приложения</returns>
    public ISnapshot CreateSnapshot(ISnapshot? previousState, IEnumerable<byte[]> deltas);
}