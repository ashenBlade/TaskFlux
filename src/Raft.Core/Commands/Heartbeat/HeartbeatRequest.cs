namespace Raft.Core.Commands.Heartbeat;

public record HeartbeatRequest(Term Term, int LeaderCommit, PeerId LeaderId, LogEntry PrevLogEntry)
{
    /// <summary>
    /// Терм лидера
    /// </summary>
    public Term Term { get; set; } = Term;

    /// <summary>
    /// ID лидера
    /// </summary>
    public PeerId LeaderId { get; set; } = LeaderId;

    /// <summary>
    /// Информация о последней записе в логе лидера
    /// </summary>
    public LogEntry PrevLogEntry { get; set; } = PrevLogEntry;

    /// <summary>
    /// Индекс последнего закомиченной записи в логе лидера
    /// </summary>
    public int LeaderCommit { get; set; } = LeaderCommit;
}