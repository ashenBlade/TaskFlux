using Raft.Core;

namespace Raft.Storage.File;

/// <summary>
/// Информация о записи в логе файла
/// </summary>
/// <param name="Term">Терм записи</param>
/// <param name="Position">Позиция записи в файле</param>
internal readonly record struct PositionTerm(Term Term, long Position);