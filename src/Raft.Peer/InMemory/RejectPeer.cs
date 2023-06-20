using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;

namespace Raft.Peer.InMemory;

public class RejectPeer: IPeer
{
    private readonly TimeSpan _responseTimeout;

    public RejectPeer(int id, TimeSpan responseTimeout)
    {
        _responseTimeout = responseTimeout;
        Id = new NodeId(id);
    }

    public NodeId Id { get; }
    
    public async Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        await Task.Delay(_responseTimeout, token);
        return AppendEntriesResponse.Fail(request.Term);
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        await Task.Delay(_responseTimeout, token);
        return new RequestVoteResponse(CurrentTerm: request.CandidateTerm, VoteGranted: false);
    }
}