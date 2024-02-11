using TaskFlux.Core;

namespace TaskFlux.Consensus;

public interface IPersistence
{
    /// <summary>
    /// Последняя запись в логе, включая незакоммиченные записи и запись в снапшоте.
    /// </summary>
    LogEntryInfo LastEntry { get; }

    /// <summary>
    /// Индекс последней закоммиченной записи
    /// </summary>
    Lsn CommitIndex { get; }

    /// <summary>
    /// Терм, сохраненный в файле метаданных
    /// </summary>
    Term CurrentTerm { get; }

    /// <summary>
    /// Отданный голос, сохраненный в файле метаданных
    /// </summary>
    NodeId? VotedFor { get; }

    /// <summary>
    /// Проверить, что переданный лог (префикс) находится в не менее актуальном состоянии чем наш 
    /// </summary>
    /// <param name="prefix">Последний элемент сравниваемого лога</param>
    /// <returns><c>true</c> - лог в нормальном состоянии, <c>false</c> - переданный лог отстает</returns>
    /// <remarks>Вызывается в RequestVote для проверки актуальности лога</remarks>
    bool IsUpToDate(LogEntryInfo prefix);

    /// <summary>
    /// Добавить в лог переданные записи, начиная с <see cref="startIndex"/> индекса.
    /// Возможно затирание старых записей.
    /// </summary>
    /// <param name="entries">Записи, которые необходимо добавить</param>
    /// <param name="startIndex">Индекс, начиная с которого необходимо добавить записи (включительно)</param>
    void InsertRange(IReadOnlyList<LogEntry> entries, Lsn startIndex);

    /// <summary>
    /// Добавить в лог одну запись
    /// </summary>
    /// <param name="entry">Запись лога</param>
    /// <returns>Индекс добавленной записи</returns>
    Lsn Append(LogEntry entry);

    /// <summary>
    /// Содержит ли текущий лог все элементы до указанного индекса включительно
    /// </summary>
    /// <param name="prefix">Информация о записи в другом логе</param>
    /// <returns><c>true</c> - префиксы логов совпадают, <c>false</c> - иначе</returns>
    /// <remarks>Вызывается в AppendEntries для проверки возможности добавления новых записей</remarks>
    bool PrefixMatch(LogEntryInfo prefix);

    /// <summary>
    /// Получить все записи, начиная с указанного индекса
    /// </summary>
    /// <param name="index">Индекс, начиная с которого нужно вернуть записи</param>
    /// <param name="entries">Хранившиеся записи, начиная с указанного индекса</param>
    /// <param name="prevLogEntry">Данные о записи, предшествующей переданным</param>
    /// <returns>Список записей из лога</returns>
    /// <remarks>При выходе за границы, может вернуть пустой массив</remarks>
    bool TryGetFrom(Lsn index, out IReadOnlyList<LogEntry> entries, out LogEntryInfo prevLogEntry);

    /// <summary>
    /// Закоммитить лог по переданному индексу
    /// </summary>
    /// <param name="index">Индекс новой закоммиченной записи</param>
    /// <returns>Результат коммита лога</returns>
    void Commit(Lsn index);

    /// <summary>
    /// Обновить состояние узла.
    /// Вызывается когда состояние (роль) узла меняется и
    /// нужно обновить голос/терм.
    /// </summary>
    /// <param name="newTerm">Новый терм</param>
    /// <param name="votedFor">Id узла, за который отдали голос</param>
    public void UpdateState(Term newTerm, NodeId? votedFor);

    /// <summary>
    /// Получить снапшот состояния, если файл существовал
    /// </summary>
    /// <param name="snapshot">Хранившийся файл снапшота</param>
    /// <param name="lastLogEntry">Последняя запись, включенная в снапшот</param>
    /// <returns><c>true</c> - файл снапшота существовал, <c>false</c> - файл снапшота не существовал</returns>
    public bool TryGetSnapshot(out ISnapshot snapshot, out LogEntryInfo lastLogEntry);

    /// <summary>
    /// Следует ли создавать новый снапшот приложения
    /// </summary>
    public bool ShouldCreateSnapshot();

    /// <summary>
    /// Прочитать из лога все закоммиченные команды, начиная с первой, включенной в снапшот.
    /// Вызывается, когда необходимо создать новый снапшот 
    /// </summary>
    /// <returns>Перечисление закоммиченных команд, начиная с команды после снапшота до индекса коммита</returns>
    public IEnumerable<byte[]> ReadCommittedDeltaFromPreviousSnapshot();

    /// <summary>
    /// Прочитать из лога все команды, начиная с первой, включенной в снапшот.
    /// Вызывается, когда инициализируется лидер для восстановления актуального состояния -
    /// можно не учитывать индекс коммита, т.к. лидер свои записи не перетирает
    /// </summary>
    /// <returns>Перечисление команд, начиная с команды после снапшота</returns>
    public IEnumerable<byte[]> ReadDeltaFromPreviousSnapshot();

    /// <summary>
    /// Создать объект для записи нового снапшота
    /// </summary>
    /// <param name="lastIncludedSnapshotEntry">Последняя включенная запись в снапшот</param>
    /// <returns>Объект для создания нового снапшота</returns>
    public ISnapshotInstaller CreateSnapshot(LogEntryInfo lastIncludedSnapshotEntry);

    /// <summary>
    /// Получить информацию о записи по указанному индексу
    /// </summary>
    /// <param name="index">Индекс записи</param>
    /// <returns>Информация о записи для которой нужно получить информацию</returns>
    public LogEntryInfo GetEntryInfo(Lsn index);
}