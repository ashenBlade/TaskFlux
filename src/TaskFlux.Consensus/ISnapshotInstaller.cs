namespace TaskFlux.Consensus;

/// <summary>
/// Интерфейс для установки файла снапшота
/// </summary>
public interface ISnapshotInstaller
{
    /// <summary>
    /// Установить очередной чанк снапшота (добавить в конец)
    /// </summary>
    /// <param name="chunk">Чанк данных, который нужно установить</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> отменен</exception>
    public void InstallChunk(ReadOnlySpan<byte> chunk, CancellationToken token);

    /// <summary>
    /// Сохранить записанное содержимое снапшота
    /// </summary>
    public void Commit();

    /// <summary>
    /// Отменить все записанные изменения
    /// </summary>
    public void Discard();
}