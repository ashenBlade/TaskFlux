namespace Raft.Core.Log;

/// <summary>
/// Реазультат проверки вхождения записи лога 
/// </summary>
public enum LogEntryCheckResult
{
    /// <summary>
    /// Запись найдена
    /// </summary>
    Contains,
    /// <summary>
    /// Запись не найдена: лог не полный
    /// </summary>
    NotFound,
    /// <summary>
    /// Текущие записи конфликтуют с требуемой
    /// </summary>
    Conflict,
}