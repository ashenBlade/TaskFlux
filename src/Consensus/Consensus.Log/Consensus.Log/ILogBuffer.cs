using Consensus.Core.Log;

namespace Consensus.Log;

public interface ILogBuffer
{
    public LogEntryInfo LastEntryInfo { get; }
    
    public LogEntryInfo GetAt(int index);

    public bool TryGetPrecedingLogEntryInfo(int nextIndex, out LogEntryInfo entry);

    /// <summary>
    /// Получить записи, хранившиеся в буфере, начиная с указанного индекса
    /// </summary>
    /// <param name="index">Индекс, начиная с которого прочитать записи</param>
    /// <returns>Хранившиеся записи лога</returns>
    public IReadOnlyList<LogEntry> ReadFrom(int index);

    /// <summary>
    /// Получить все хранимые записи лога до указанного индекса включительно 
    /// </summary>
    /// <param name="index">Индекс записи лога</param>
    /// <returns>Хранящиеся записи лога</returns>
    public List<LogEntry> TrimUntil(int index);

    /// <summary>
    /// Получить все хранившиеся записи в логе и удалить их из буфера
    /// </summary>
    /// <returns>Хранившиеся записи лога</returns>
    public List<LogEntry> TrimAll();

    /// <summary>
    /// Добавить запись в конец очереди
    /// </summary>
    /// <param name="entry">Новая запись</param>
    /// <returns>Информация о новой записи</returns>
    public LogEntryInfo Append(LogEntry entry);

    /// <summary>
    /// Вставить несколько записей, начиная с указанного индекса
    /// </summary>
    /// <param name="entries">Записи для вставки</param>
    /// <param name="start">Индекс, начиная с которого добавлять записи</param>
    public void InsertRange(IEnumerable<LogEntry> entries, int start);

    /// <summary>
    /// Получить последнуюю запись в указанном логе
    /// </summary>
    /// <param name="info">Последняя ханящаяся запись в логе</param>
    /// <returns><c>true</c> - запись была получена, <c>false</c> - иначе</returns>
    public bool TryPeekLast(out LogEntryIndex info);
}