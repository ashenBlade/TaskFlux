namespace Consensus.Raft.Persistence.LogFileCheckStrategy;

public interface ILogFileSizeChecker
{
    /// <summary>
    /// Проверить превысил ли размер файла лога максимальное значение
    /// </summary>
    /// <param name="logFileSize">Текущий размер файла лога</param>
    /// <returns><c>true</c> - размер превышен, <c>false</c> - иначе</returns>
    public bool IsLogFileSizeExceeded(ulong logFileSize);
}