using Consensus.StateMachine;

namespace Consensus.Core.Log;

public interface ILog
{
    public IReadOnlyList<LogEntry> ReadLog();

    /// <summary>
    /// Содержит ли текущий лог все элементы до указанного индекса включительно
    /// </summary>
    /// <param name="prefix">Информация о записи в другом логе</param>
    /// <returns><c>true</c> - префиксы логов совпадают, <c>false</c> - иначе</returns>
    /// <remarks>Вызывается в AppendEntries для проверки возможности добавления новых записей</remarks>
    public bool Contains(LogEntryInfo prefix);

    /// <summary>
    /// Конфликтует ли текущий лог с переданным (используется префикс)
    /// </summary>
    /// <param name="prefix">Последний элемент сравниваемого лога</param>
    /// <returns><c>true</c> - конфликтует, <c>false</c> - иначе</returns>
    /// <remarks>Вызывается в RequestVote для проверки актуальности лога</remarks>
    public bool Conflicts(LogEntryInfo prefix);

    /// <summary>
    /// Добавить в лог переданные записи, начиная с <see cref="startIndex"/> индекса.
    /// Возможно перезатирание 
    /// </summary>
    /// <param name="entries">Записи, которые необходимо добавить</param>
    /// <param name="startIndex">Индекс, начиная с которого необходимо добавить записи</param>
    public void InsertRange(IEnumerable<LogEntry> entries, int startIndex);
    
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
    /// <remarks>Используется 0-based индекс, а в оригинальном RAFT - 1-based</remarks>
    public IReadOnlyList<LogEntry> GetFrom(int index);

    /// <summary>
    /// Закоммитить лог по переданному индексу
    /// </summary>
    /// <param name="index">Индекс новой закомиченной записи</param>
    /// <returns>Результат коммита лога</returns>
    public void Commit(int index);
    
    /// <summary>
    /// Получить информацию о записи, предшествующей указанной
    /// </summary>
    /// <param name="nextIndex">Индекс следующей записи</param>
    /// <returns>Информацию о следующей записи в логе</returns>
    /// <remarks>Если указанный индекс 0, то вернется <see cref="LogEntryInfo.Tomb"/></remarks>
    public LogEntryInfo GetPrecedingEntryInfo(int nextIndex);
    
    /// <summary>
    /// Применить непримененные записи к указанной машине состояний
    /// </summary>
    /// <param name="stateMachine">Машина состояний, для которой нужно применить команды</param>
    public void ApplyCommitted(IStateMachine stateMachine);

    /// <summary>
    /// Указать новый индекс последней примененной записи
    /// </summary>
    /// <param name="index">Индекс записи в логе</param>
    void SetLastApplied(int index);
}