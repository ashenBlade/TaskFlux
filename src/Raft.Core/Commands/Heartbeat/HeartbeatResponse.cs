namespace Raft.Core.Commands.Heartbeat;

/// <summary>
/// Ответ на Heartbeat запрос
/// </summary>
/// <param name="Term">
/// Текущий терм узла.
/// Возможно после обновления состояния
/// </param>
/// <param name="Success">
/// Мой лог может быть синхронизирован
/// </param>
public record HeartbeatResponse(Term Term, bool Success)
{
    public static HeartbeatResponse Ok(Term term) => new(term, true);
    public static HeartbeatResponse Fail(Term term) => new(term, false);
}