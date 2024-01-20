namespace TaskFlux.Consensus.Persistence;

/// <summary>
/// Структура представляющая собой информацию о какой либо записи команды - ее терм и индекс
/// </summary>
/// <param name="Term">Терм команды</param>
/// <param name="Index">Индекс команды</param>
public readonly record struct LogEntryInfo(Term Term, int Index)
{
    /// <summary>
    /// Используется для указания пустого лога.
    /// В оригинальной статье используется индексирование с 1.
    /// Для удобства я использую индексирование с 0.
    /// </summary>
    public const int TombIndex = -1;

    /// <summary>
    /// Конструктор без параметров лучше не использовать.
    /// Юзай с параметрами, этот только чтобы ошибок было меньше.
    /// Создается Tomb по умолчанию.
    /// </summary>
    public LogEntryInfo() : this(Term.Start, TombIndex)
    {
    }

    /// <summary>
    /// Пустая запись.
    /// Используется когда в логе нет записей, чтобы не вводить <c>null</c>
    /// </summary>
    public static readonly LogEntryInfo Tomb = new(Term.Start, TombIndex);

    /// <summary>
    /// Является ли запись меткой пустого лога.
    /// Такая запись сигнализирует о том, что в логе нет записей
    /// </summary>
    public bool IsTomb => Index == TombIndex;

    public override string ToString()
    {
        if (IsTomb)
        {
            return "LogEntryInfo(Tomb)";
        }

        return $"LogEntryInfo(Index = {Index}; Term = {Term.Value})";
    }
}