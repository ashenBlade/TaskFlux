namespace Raft.Core.Commands.RequestVote;

public record RequestVoteRequest(PeerId CandidateId, Term CandidateTerm, LogEntry LastLog)
{
    /// <summary>
    /// Term кандидата, который послал запрос
    /// </summary>
    public Term CandidateTerm { get; set; } = CandidateTerm;

    /// <summary>
    /// ID кандидата, который послал запрос
    /// </summary>
    public PeerId CandidateId { get; set; } = CandidateId;

    /// <summary>
    /// Информация о последнем логе друго узла
    /// </summary>
    public LogEntry LastLog { get; set; } = LastLog;
}