namespace Raft.Core.Commands.Heartbeat;

public class HeartbeatRequest
{
    /// <summary>
    /// Терм лидера
    /// </summary>
    public Term Term { get; set; }

    /// <summary>
    /// ID лидера
    /// </summary>
    public PeerId LeaderId { get; set; }

    /// <summary>
    /// Информация о последней записе в логе лидера
    /// </summary>
    public LogEntry PrevLogEntry { get; set; }
    
    /// <summary>
    /// Индекс последнего закомиченной записи в логе лидера
    /// </summary>
    public int LeaderCommit { get; set; }
}