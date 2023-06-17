using Raft.Core.Log;

namespace Raft.Core.Commands.Heartbeat;

/// <summary>
/// Запрос Heartbeat
/// </summary>
/// <param name="Term">Терм узла, пославшего Heartbeat</param>
/// <param name="LeaderCommit">Индекс последнего закомиченной записи в логе лидера</param>
/// <param name="LeaderId">ID узла, пославшего Heartbeat</param>
/// <param name="PrevLogEntry">Информация о последней записе в логе лидера</param>
public record HeartbeatRequest(Term Term, int LeaderCommit, NodeId LeaderId, LogEntry PrevLogEntry);