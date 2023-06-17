namespace Raft.Core.Log;

public readonly record struct LogEntry(Term Term, int Index)
{
    /// <summary>
    /// Пустая запись.
    /// Используется когда в логе нет записей, чтобы не вводить <c>null</c>
    /// </summary>
    public static readonly LogEntry Empty = new(Term.Start, 0);
    
    /// <summary>
    /// Может ли текущий лог быть актуализирован другим (содержится ли в другом логе)
    /// </summary>
    /// <param name="other">Последняя запись в другом логе</param>
    /// <returns><c>true</c> - текущий лог может принимать обновления другого лога, иначе <c>false</c></returns>
    public bool IsUpToDateWith(LogEntry other) => 
        // (this.Term, this.LastIndex) < (other.Term, other.LastIndex) 
        Term == other.Term
            ? Index <= other.Index
            : Term < other.Term;
}