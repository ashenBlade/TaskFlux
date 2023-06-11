using Raft.Core.Commands;

namespace Raft.Core.StateMachine;

/// <summary>
/// Интерфейс, представляющий конкретное состояние узла
/// </summary>
/// <remarks>
/// IDisposable нужно вызывать для сброса таймеров и очистки ресурсов предыдущего состояния
/// </remarks>
internal interface INodeState: IDisposable
{
    public Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token);
    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token);
}