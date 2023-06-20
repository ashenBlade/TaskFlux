namespace Raft.Core.Log;

public interface ILog
{
    public LogEntry Append(Term term);
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
    
    /// <summary>
    /// Индекс последней закомиченной записи
    /// </summary>
    public int CommitIndex { get; set; }
    
    /// <summary>
    /// Индекс последней применной записи журнала 
    /// </summary>
    public int LastApplied { get; }
}