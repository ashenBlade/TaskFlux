namespace Consensus.Storage.File;

public class Constants
{
    /// <summary>
    /// Маркер файла, относящегося к приложению.
    /// В каждом файле первые 4 байта должны быть равны ему.
    /// </summary>
    internal const int Marker = 0x2F6F0F2F;

    /// <summary>
    /// Название файла с метаданными
    /// </summary>
    public const string MetadataFileName = "raft.metadata";

    /// <summary>
    /// Название файла лога команд
    /// </summary>
    public const string LogFileName = "raft.log";

    /// <summary>
    /// Название файла снапшота
    /// </summary>
    public const string SnapshotFileName = "raft.snapshot";
}