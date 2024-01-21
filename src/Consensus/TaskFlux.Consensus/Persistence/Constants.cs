namespace TaskFlux.Consensus.Persistence;

public class Constants
{
    /// <summary>
    /// Название файла с метаданными
    /// </summary>
    public const string MetadataFileName = "tflux.metadata";

    /// <summary>
    /// Название файла лога команд
    /// </summary>
    public const string LogFileName = "tflux.log";

    /// <summary>
    /// Название файла снапшота
    /// </summary>
    public const string SnapshotFileName = "tflux.snapshot";

    /// <summary>
    /// Название директории для временных файлов
    /// </summary>
    public const string TemporaryDirectoryName = "temporary";

    /// <summary>
    /// Максимальный размер файла лога по умолчанию
    /// </summary>
    public const ulong MaxLogFileSize = 16    // Мб
                                      * 1024  // Кб
                                      * 1024; // б
}