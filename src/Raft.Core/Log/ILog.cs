namespace Raft.Core.Log;

public interface ILog
{
    public IReadOnlyList<LogEntry> Entries { get; }

    /// <summary>
    /// Проверить консистентность лога для указанного префикса лога
    /// </summary>
    /// <param name="prefix">Запись в другом логе</param>
    /// <returns><c>true</c> - префиксы логов совпадают, <c>false</c> - иначе</returns>
    public bool IsConsistentWith(LogEntryInfo prefix);

    /// <summary>
    /// Добавить в лог переданные записи, начиная с <see cref="startIndex"/> индекса.
    /// Возможно перезатирание 
    /// </summary>
    /// <param name="entries">Записи, которые необходимо добавить</param>
    /// <param name="startIndex">Индекс, начиная с которого необходимо добавить записи</param>
    public void AppendUpdateRange(IEnumerable<LogEntry> entries, int startIndex);
    
    /// <summary>
    /// Добавить в лог одну запись
    /// </summary>
    /// <param name="entry">Запись лога</param>
    /// <returns>Информация о добавленной записи</returns>
    public LogEntryInfo Append(LogEntry entry);

    /// <summary>
    /// Последняя запись в логе
    /// </summary>
    public LogEntryInfo LastEntry { get; }
    
    /// <summary>
    /// Индекс последней закомиченной записи
    /// </summary>
    public int CommitIndex { get; }
    
    /// <summary>
    /// Индекс последней применной записи журнала.
    /// Обновляется после успешного коммита 
    /// </summary>
    public int LastApplied { get; }

    /// <summary>
    /// Получить все записи, начиная с указанного индекса
    /// </summary>
    /// <param name="index">Индекс, начиная с которого нужно вернуть записи</param>
    /// <returns>Список записей из лога</returns>
    /// <remarks>При выходе за границы, может вернуть пустой массив</remarks>
    public IReadOnlyList<LogEntry> GetFrom(int index);

    /// <summary>
    /// Закоммитить лог по переданному индексу
    /// </summary>
    /// <param name="index">Индекс новой закомиченной записи</param>
    public void Commit(int index);
}