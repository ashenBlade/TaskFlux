using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;

namespace Raft.Core.StateMachine;

/// <summary>
/// Интерфейс, представляющий конкретное состояние узла
/// </summary>
/// <remarks>
/// IDisposable нужно вызывать для сброса таймеров и очистки ресурсов предыдущего состояния
/// </remarks>
public interface INodeState: IDisposable
{
    public NodeRole Role { get; }
    public RequestVoteResponse Apply(RequestVoteRequest request);
    public HeartbeatResponse Apply(HeartbeatRequest request);
}