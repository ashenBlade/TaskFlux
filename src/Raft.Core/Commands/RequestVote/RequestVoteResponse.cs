namespace Raft.Core.Commands.RequestVote;

/// <summary>
/// Ответ на команду <see cref="RequestVoteRequest"/>
/// </summary>
/// <param name="CurrentTerm">Term для обновления кандидата</param>
/// <param name="VoteGranted">
/// true - узел принял запрос
/// false - отверг
/// </param>
public record RequestVoteResponse(Term CurrentTerm, bool VoteGranted);