using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;

namespace Raft.Peer.InMemory;

public class RejectPeer: IPeer
{
    private readonly TimeSpan _responseTimeout;

    public RejectPeer(int id, TimeSpan responseTimeout)
    {
        _responseTimeout = responseTimeout;
        Id = new PeerId(id);
    }

    public PeerId Id { get; }
    
    public async Task<HeartbeatResponse?> SendHeartbeat(HeartbeatRequest request, CancellationToken token)
    {
        await Task.Delay(_responseTimeout, token);
        return HeartbeatResponse.Fail(request.Term);
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        await Task.Delay(_responseTimeout, token);
        return new RequestVoteResponse()
        {
            CurrentTerm = request.CandidateTerm, 
            VoteGranted = false
        };
    }

    public Task SendAppendEntries(CancellationToken token)
    {
        return Task.CompletedTask;
    }
}