namespace Raft.Core;

/// <summary>
/// Интерфейс, представляющий другой узел 
/// </summary>
public interface IPeer
{
    /// <summary>
    /// Идентификатор узла
    /// </summary>
    public PeerId Id { get; }
}