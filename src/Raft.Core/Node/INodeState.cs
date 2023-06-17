using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;

namespace Raft.Core.Node;

/// <summary>
/// Интерфейс, представляющий конкретное состояние узла
/// </summary>
/// <remarks>
/// IDisposable нужно вызывать для сброса таймеров и очистки ресурсов предыдущего состояния (отписка от таймеров и т.д.)
/// </remarks>
public interface INodeState: IDisposable
{
    /// <summary>
    /// Текущая роль этого состояния
    /// </summary>
    public NodeRole Role { get; }
    /// <summary>
    /// Применить команду RequestVote
    /// </summary>
    /// <param name="request">Объект запроса</param>
    /// <returns>Ответ узла</returns>
    public RequestVoteResponse Apply(RequestVoteRequest request);
    /// <summary>
    /// Применить команду Heartbeat
    /// </summary>
    /// <param name="request">Объект запроса</param>
    /// <returns>Ответ узла</returns>
    public HeartbeatResponse Apply(HeartbeatRequest request);
}