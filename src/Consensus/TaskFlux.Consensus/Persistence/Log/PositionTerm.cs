namespace TaskFlux.Consensus.Persistence.Log;

/// <summary>
/// Информация о записи в логе файла
/// </summary>
/// <param name="Term">Терм записи</param>
/// <param name="Position">Позиция записи в файле (начиная с маркера записи)</param>
internal readonly record struct PositionTerm(Term Term, long Position);