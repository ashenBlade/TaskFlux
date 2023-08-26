using TaskFlux.Core;

namespace Consensus.Raft.Persistence.Metadata;

/// <summary>
/// Объект-обертка вокруг файла метаданных рафта - `consensus/raft.metadata`
/// </summary>
public interface IMetadataStorage
{
    /// <summary>
    /// Последний сохраненный терм
    /// </summary>
    public Term ReadTerm();

    /// <summary>
    /// Последний отданный голос
    /// </summary>
    public NodeId? ReadVotedFor();

    /// <summary>
    /// Атомарно обновить метаданные узла
    /// </summary>
    /// <param name="term">Новый терм</param>
    /// <param name="votedFor">Отданный голос за</param>
    public void Update(Term term, NodeId? votedFor);
}