using Consensus.Raft.Persistence;
using TaskFlux.Core;

namespace Consensus.Raft.Commands.InstallSnapshot;

/// <summary>
/// Команда InstallSnapshotRPC из рафта.
/// Необходима для создания нового файла снапшота, когда у лидера нет нужных записей лога
/// </summary>
/// <param name="Term">Терм лидера</param>
/// <param name="LeaderId">ID лидера (тот кто посылает запрос)</param>
/// <param name="LastIncludedIndex">Последний индекс команды, которая была применена к передаваемому состоянию</param>
/// <param name="LastIncludedTerm">Терм, в котором была последняя применная команда</param>
/// <param name="Snapshot">Снапшот, который передается от лидера</param>
public record InstallSnapshotRequest(Term Term,
                                     NodeId LeaderId,
                                     int LastIncludedIndex,
                                     Term LastIncludedTerm,
                                     ISnapshot Snapshot);