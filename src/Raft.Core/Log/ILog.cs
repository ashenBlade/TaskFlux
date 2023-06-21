namespace Raft.Core.Log;

public interface ILog
{
    public LogEntryInfo Append(Term term, string command);
    /// <summary>
    /// Проверить запись лога на вхождение в существующий лог
    /// </summary>
    /// <param name="entryInfo">Информация о записи в логе</param>
    /// <returns>Результат проверки</returns>
    public LogEntryCheckResult Check(LogEntryInfo entryInfo);

    /// <summary>
    /// Последняя запись в логе
    /// </summary>
    public LogEntryInfo LastLogEntryInfo { get; }
    
    /// <summary>
    /// Индекс последней закомиченной записи
    /// </summary>
    public int CommitIndex { get; set; }
    
    /// <summary>
    /// Индекс последней применной записи журнала 
    /// </summary>
    public int LastApplied { get; }

    public IReadOnlyList<LogEntry> this[Range range] { get; }
    public IReadOnlyList<LogEntry> this[int index] { get; }
}