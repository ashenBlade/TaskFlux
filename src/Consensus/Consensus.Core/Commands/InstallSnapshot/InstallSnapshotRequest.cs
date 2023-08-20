using TaskFlux.Core;

namespace Consensus.Core.Commands.InstallSnapshot;

/// <summary>
/// Команда InstallSnapshotRPC из рафта.
/// Необходима для создания нового файла снапшота, когда у лидера нет нужных записей лога
/// </summary>
/// <param name="Term">Терм лидера</param>
/// <param name="LeaderId">ID лидера (тот кто посылает запрос)</param>
/// <param name="LastIncludedIndex">Последний индекс команды, которая была применена к передаваемому состоянию</param>
/// <param name="LastIncludedTerm">Терм, в котором была последняя применная команда</param>
/// <param name="Offset">Смещение в файла в байтах. С этой позиции нужно записывать в файл байты из <paramref name="Data"/></param>
/// <param name="Data">Данные снапшота, находящиеся в файле с позиции <paramref name="Offset"/></param>
/// <param name="Done">Это был последний чанк данных. Означает конец передачи</param>
public record InstallSnapshotRequest(Term Term,
                                     NodeId LeaderId,
                                     int LastIncludedIndex,
                                     Term LastIncludedTerm,
                                     long Offset,
                                     byte[] Data,
                                     bool Done);