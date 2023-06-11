namespace Raft.Core.Commands.Heartbeat;

public record HeartbeatResponse(Term Term, bool Success)
{
    /// <summary>
    /// Текущий терм.
    /// После получения может обновиться
    /// </summary>
    public Term Term { get; } = Term;

    /// <summary>
    /// Мой лог может быть синхронизирован
    /// </summary>
    public bool Success { get; } = Success;

    public static HeartbeatResponse Ok(Term term) => new(term, true);
    public static HeartbeatResponse Fail(Term term) => new(term, false);
}