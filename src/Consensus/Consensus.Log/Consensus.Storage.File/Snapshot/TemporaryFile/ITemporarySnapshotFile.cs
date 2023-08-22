namespace Consensus.Storage.File.Snapshot;

public interface ITemporarySnapshotFile
{
    /// <summary>
    /// Открыть временный файл снапшота
    /// </summary>
    /// <remarks>
    /// При первом вызове создается новый файл.
    /// Дальнейшие вызовы возвращают один и тот же объект
    /// </remarks>
    /// <returns>Поток, представляющий файл</returns>
    public Stream Open();

    /// <summary>
    /// Заменить существующий файл снапшота тем, который представляет этот объект, либо создать новый из текущего.
    /// </summary>
    /// <remarks>
    /// Перед вызовом нужно закрыть старый файл снапшота.
    /// В противном случае переименовать не удастся, так как файл открыт.
    /// </remarks>
    /// <exception cref="InvalidOperationException">Файл был уже сохранен, либо еще не создался (<see cref="Open"/> еще не вызывался)</exception>
    public void Commit();

    /// <summary>
    /// Удалить временный файл снапшота
    /// </summary>
    /// <exception cref="InvalidOperationException"><see cref="Commit"/> был вызван, либо файл еще не существует (<see cref="Open"/> не вызывался)</exception>
    public void Delete();
}