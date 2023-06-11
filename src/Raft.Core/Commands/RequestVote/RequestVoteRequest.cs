namespace Raft.Core.Commands;

public class RequestVoteRequest
{
    /// <summary>
    /// Term кандидата, который послал запрос
    /// </summary>
    public Term CandidateTerm { get; set; }
    /// <summary>
    /// ID кандидата, который послал запрос
    /// </summary>
    public PeerId CandidateId { get; set; }

    /// <summary>
    /// Информация о последнем логе друго узла
    /// </summary>
    public LogEntry LastLog { get; set; }
}