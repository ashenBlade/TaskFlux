namespace Consensus.Raft.Commands.InstallSnapshot;

/// <summary>
/// Ответ на <see cref="InstallSnapshotRequest"/> запрос лидера
/// </summary>
/// <param name="CurrentTerm">Терм узла, которому посылался запрос. Нужен, чтобы лидер мог обновиться</param>
public sealed record InstallSnapshotResponse(Term CurrentTerm);