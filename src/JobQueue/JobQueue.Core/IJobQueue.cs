namespace JobQueue.Core;

public interface IJobQueue : IReadOnlyJobQueue
{
    /// <summary>
    /// Добавить новый элемент в очередь
    /// </summary>
    /// <param name="key">Ключ</param>
    /// <param name="payload">Данные</param>
    /// <returns><c>true</c> - значение было добавлено в очередь, <c>false</c> - в очереди нет места для новых элементов</returns>
    public bool TryEnqueue(long key, byte[] payload);

    /// <summary>
    /// Получить элемент из очереди
    /// </summary>
    /// <param name="key">Полученный ключ</param>
    /// <param name="payload">Полученные данные</param>
    /// <returns><c>true</c> - элемент получен, <c>false</c> - очередь была пуста и ничего не получено</returns>
    /// <remarks>Если ничего не получено, то <paramref name="key"/> и <paramref name="payload"/> будут хранить <c>0</c> и <c>null</c></remarks>
    public bool TryDequeue(out long key, out byte[] payload);
}