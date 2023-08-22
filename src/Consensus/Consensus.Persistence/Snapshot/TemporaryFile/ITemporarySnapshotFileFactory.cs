namespace Consensus.Storage.File.Snapshot;

/// <summary>
/// Класс для создания новых временных файлов снапшота.
/// Нужно для абстракции от файловой системы
/// </summary>
public interface ITemporarySnapshotFileFactory
{
    /// <summary>
    /// Метод для создания нового временного файла снапшота.
    /// </summary>
    /// <returns>Объект временного файла снапшота</returns>
    public ITemporarySnapshotFile CreateTempSnapshotFile();
}