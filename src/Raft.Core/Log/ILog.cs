namespace Raft.Core.Log;

public interface ILog
{
    /// <summary>
    /// Проверить запись лога на вхождение в существующий лог
    /// </summary>
    /// <param name="entry">Информация о записи в логе</param>
    /// <returns>Результат проверки</returns>
    public LogEntryCheckResult Check(LogEntry entry);

    /// <summary>
    /// Последняя запись в логе
    /// </summary>
    public LogEntry LastLogEntry { get; }
}