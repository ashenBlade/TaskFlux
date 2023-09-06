using Consensus.Raft.Persistence;
using TaskFlux.Core;

namespace Consensus.Raft.Commands.InstallSnapshot;

/// <summary>
/// Команда InstallSnapshotRPC из рафта.
/// Необходима для создания нового файла снапшота, когда у лидера нет нужных записей лога
/// </summary>
/// <param name="Term">Терм лидера</param>
/// <param name="LeaderId">ID лидера (тот кто посылает запрос)</param>
/// <param name="Snapshot">Снапшот, который передается от лидера</param>
/// <param name="LastEntry">Последняя команда, примененная в снапшоте</param>
public record InstallSnapshotRequest(Term Term,
                                     NodeId LeaderId,
                                     LogEntryInfo LastEntry,
                                     ISnapshot Snapshot);