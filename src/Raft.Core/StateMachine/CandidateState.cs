using Raft.Core.Commands;

namespace Raft.Core.StateMachine;

public class CandidateState: INodeState
{
    public async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }
}