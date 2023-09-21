using Consensus.Raft.Persistence;

namespace Consensus.Raft;

public interface IApplicationFactory<in TCommand, out TResponse>
{
    /// <summary>
    /// Создать новое (чистое) приложение.
    /// Используется при старте приложения, когда снапшота не существовало.
    /// </summary>
    /// <returns>Новое приложение</returns>
    public IApplication<TCommand, TResponse> CreateEmpty();

    /// <summary>
    /// Восстановить приложение из переданного снапшота
    /// </summary>
    /// <param name="snapshot">Снапшот приложения</param>
    /// <returns>Восстановленное приложение</returns>
    /// <exception cref="InvalidDataException">Переданный снапшот содержит невалидные данные</exception>
    public IApplication<TCommand, TResponse> Restore(ISnapshot snapshot);
}