using Raft.Core.Log;

namespace Raft.Core.Commands.RequestVote;

/// <summary>
/// Запрос RequestVote
/// </summary>
/// <param name="CandidateId">ID кандидата, который послал запрос</param>
/// <param name="CandidateTerm">Терм кандидата, который послал запрос</param>
/// <param name="LastLog">Информация о последнем логе друго узла</param>
public record RequestVoteRequest(NodeId CandidateId, Term CandidateTerm, LogEntry LastLog);