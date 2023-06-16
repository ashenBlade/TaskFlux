using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Peer;

namespace Raft.Peer.InMemory;

public class LambdaPeer: IPeer
{
    private readonly Func<HeartbeatRequest, Task<HeartbeatResponse?>> _sendHeartbeatHandler;
    private readonly Func<RequestVoteRequest, Task<RequestVoteResponse?>> _requestVoteHandler;
    public PeerId Id { get; }

    public LambdaPeer(int id, Func<HeartbeatRequest, Task<HeartbeatResponse?>> sendHeartbeatHandler, Func<RequestVoteRequest, Task<RequestVoteResponse?>> requestVoteHandler)
    {
        _sendHeartbeatHandler = sendHeartbeatHandler;
        _requestVoteHandler = requestVoteHandler;
        Id = new(id);
    }
    
    public Task<HeartbeatResponse?> SendHeartbeat(HeartbeatRequest request, CancellationToken token)
    {
        return _sendHeartbeatHandler(request);
    }

    public Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        return _requestVoteHandler(request);
    }

    public Task SendAppendEntries(CancellationToken token)
    {
        throw new NotImplementedException();
    }
}