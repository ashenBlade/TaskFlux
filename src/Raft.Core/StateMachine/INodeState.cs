using Raft.Core.Commands;

namespace Raft.Core.StateMachine;

public interface INodeState
{
    public Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token);
    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token);
}