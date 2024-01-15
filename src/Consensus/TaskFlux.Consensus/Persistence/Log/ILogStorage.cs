namespace TaskFlux.Consensus.Persistence.Log;

/// <summary>
/// Объект-обертка над файлом лога команд - `consensus/raft.log`
/// </summary>
public interface ILogStorage
{
    /// <summary>
    /// Количество записей команд в логе
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Размер файла лога
    /// </summary>
    ulong FileSize { get; }

    /// <summary>
    /// Добавить одну запись в конец лога
    /// </summary>
    /// <param name="entry">Запись, которую необходимо добавить</param>
    /// <returns>Информация о последней (вставленной) записи лога</returns>
    public LogEntryInfo Append(LogEntry entry);

    /// <summary>
    /// Добавить в хранилище несколько записей лога
    /// </summary>
    /// <param name="entries">Записи, которые необходимо добавить</param>
    /// <returns>Информация о последней записе лога</returns>
    public LogEntryInfo AppendRange(IEnumerable<LogEntry> entries);

    /// <summary>
    /// Получить все записи, хранящиеся в логе
    /// </summary>
    /// <returns>Все записи из лога</returns>
    public IReadOnlyList<LogEntry> ReadAll();

    /// <summary>
    /// Получить информацию о предшествующей записе из лога
    /// </summary>
    /// <param name="nextIndex">Индекс, следующий перед необходимым</param>
    /// <returns>Информация о предыдущей записи лога</returns>
    public LogEntryInfo GetPrecedingLogEntryInfo(int nextIndex);

    /// <summary>
    /// Получить последнюю запись в логе
    /// </summary>
    /// <returns>Последняя запись в логе или <see cref="LogEntryInfo.Tomb"/> если лог пустой</returns>
    public LogEntryInfo GetLastLogEntry();

    /// <summary>
    /// Получить все записи лога, начиная с <paramref name="startIndex"/> индекса 
    /// </summary>
    /// <param name="startIndex">Индекс, начиная с которого получить записи лога</param>
    /// <returns>Записи лога</returns>
    public IReadOnlyList<LogEntry> ReadFrom(int startIndex);

    /// <summary>
    /// Получить информацию о записи по индексу
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public LogEntryInfo GetInfoAt(int index);

    /// <summary>
    /// Получить срез лога с <paramref name="start"/> до <paramref name="end"/> индекса включительно
    /// </summary>
    /// <param name="start">Индекс начала включительно</param>
    /// <param name="end">Индекс конца включительно</param>
    /// <returns>Срез лога на указанных границах</returns>
    IReadOnlyList<LogEntry> GetRange(int start, int end);

    /// <summary>
    /// Полностью очистить лог команд.
    /// Вызывается когда размер лога превысил максимальный и снашот состояния сброшен на диск 
    /// </summary>
    public void Clear();
}