using Consensus.Core.Persistence;

namespace Consensus.Core.Log;

/// <summary>
/// Объект доступа к пресистентным структурам данных: лог команд, метаданные, снапшот
/// </summary>
public interface IPersistenceManager
{
    /// <summary>
    /// Прочитать все записи лога
    /// </summary>
    /// <returns>Список хранящихся записей лога</returns>
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
    public int LastAppliedIndex { get; }

    /// <summary>
    /// Размер файла лога, включая заголовки
    /// </summary>
    public ulong LogFileSize { get; }

    /// <summary>
    /// Последняя примененная запись из лога
    /// </summary>
    public LogEntryInfo LastApplied { get; }

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
    /// Получить закомиченные, но еще не примененные записи из лога.
    /// Это записи, индекс которых находится между индексом последней применненной записи (<see cref="LastAppliedIndex"/>) и
    /// последней закоммиченной записи (<see cref="CommitIndex"/>) 
    /// </summary>
    /// <returns>Записи, которые были закомичены, но еще не применены</returns>
    public IReadOnlyList<LogEntry> GetNotApplied();

    /// <summary>
    /// Указать новый индекс последней примененной записи
    /// </summary>
    /// <param name="index">Индекс записи в логе</param>
    void SetLastApplied(int index);

    /// <summary>
    /// Перезаписать старый снапшот новым и обновить файл лога (удалить старые записи)
    /// </summary>
    /// <param name="lastLogEntry">
    /// Информация о последней примененной команде в <paramref name="snapshot"/>.
    /// Этот метод только сохраняет новый файл снапшота, без очищения или удаления лога команд
    /// </param>
    /// <param name="snapshot">Слепок системы</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    public void SaveSnapshot(LogEntryInfo lastLogEntry, ISnapshot snapshot, CancellationToken token = default);

    /// <summary>
    /// Очистить файл лога команд.
    /// Выполняется после создания нового снапшота.
    /// </summary>
    public void ClearCommandLog();
    
    /// <summary>
    /// Получить снапшот состояния, если файл существовал
    /// </summary>
    /// <param name="snapshot">Хранившийся файл снапшота</param>
    /// <returns><c>true</c> - файл снапшота существовал, <c>false</c> - файл снапшота не существовал</returns>
    public bool TryGetSnapshot(out ISnapshot snapshot);
}