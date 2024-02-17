using TaskFlux.Core;

namespace TaskFlux.Consensus.Commands.RequestVote;

/// <summary>
/// Запрос RequestVote
/// </summary>
/// <param name="CandidateId">ID кандидата, который послал запрос</param>
/// <param name="CandidateTerm">Терм кандидата, который послал запрос</param>
/// <param name="LastLogEntryInfo">Информация о последнем логе друго узла</param>
public record RequestVoteRequest(NodeId CandidateId, Term CandidateTerm, LogEntryInfo LastLogEntryInfo);