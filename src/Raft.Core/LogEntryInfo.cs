namespace Raft.Core;

public readonly record struct LogEntryInfo(Term Term, int Index)
{
    /// <summary>
    /// Может ли текущий лог быть актуализирован другим (содержится ли в другом логе)
    /// </summary>
    /// <param name="other">Последняя запись в другом логе</param>
    /// <returns><c>true</c> - текущий лог может принимать обновления другого лога, иначе <c>false</c></returns>
    public bool IsUpToDateWith(LogEntryInfo other) => 
        // (this.Term, this.LastIndex) < (other.Term, other.LastIndex) 
        Term == other.Term
            ? Index <= other.Index
            : Term < other.Term;
}