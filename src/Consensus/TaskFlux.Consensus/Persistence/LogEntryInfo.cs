namespace TaskFlux.Consensus.Persistence;

/// <summary>
/// Структура представляющая собой информацию о какой либо записи команды - ее терм и индекс
/// </summary>
/// <param name="Term">Терм команды</param>
/// <param name="Index">Индекс команды</param>
public readonly record struct LogEntryInfo(Term Term, Lsn Index)
{
    /// <summary>
    /// Пустая запись.
    /// Используется когда в логе нет записей, чтобы не вводить <c>null</c>
    /// </summary>
    public static readonly LogEntryInfo Tomb = new(Term.Start, Lsn.Tomb);

    /// <summary>
    /// Является ли запись меткой пустого лога.
    /// Такая запись сигнализирует о том, что в логе нет записей
    /// </summary>
    public bool IsTomb => Index.IsTomb;

    public override string ToString()
    {
        if (IsTomb)
        {
            return "LogEntryInfo(Tomb)";
        }

        return $"LogEntryInfo(Index = {Index}; Term = {Term.Value})";
    }
}