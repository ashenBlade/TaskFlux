using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;

namespace Raft.Peer.InMemory;

public class LambdaPeer: IPeer
{
    private readonly Func<AppendEntriesRequest, Task<AppendEntriesResponse?>> _sendAppendEntriesHandler;
    private readonly Func<RequestVoteRequest, Task<RequestVoteResponse?>> _requestVoteHandler;
    public NodeId Id { get; }

    public LambdaPeer(int id, Func<AppendEntriesRequest, Task<AppendEntriesResponse?>> sendAppendEntriesHandler, Func<RequestVoteRequest, Task<RequestVoteResponse?>> requestVoteHandler)
    {
        _sendAppendEntriesHandler = sendAppendEntriesHandler;
        _requestVoteHandler = requestVoteHandler;
        Id = new(id);
    }
    

    public Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        return _sendAppendEntriesHandler(request);
    }

    public Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        return _requestVoteHandler(request);
    }
}