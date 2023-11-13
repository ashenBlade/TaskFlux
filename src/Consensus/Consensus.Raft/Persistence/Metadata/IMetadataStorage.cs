using TaskFlux.Models;

namespace Consensus.Raft.Persistence.Metadata;

/// <summary>
/// Объект-обертка вокруг файла метаданных рафта - `consensus/raft.metadata`
/// </summary>
public interface IMetadataStorage
{
    /// <summary>
    /// Последний сохраненный терм
    /// </summary>
    public Term Term { get; }

    /// <summary>
    /// Последний отданный голос
    /// </summary>
    public NodeId? VotedFor { get; }

    /// <summary>
    /// Атомарно обновить метаданные узла
    /// </summary>
    /// <param name="term">Новый терм</param>
    /// <param name="votedFor">Отданный голос за</param>
    public void Update(Term term, NodeId? votedFor);
}