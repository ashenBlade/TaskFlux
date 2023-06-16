using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Peer;

namespace Raft.Server.Infrastructure;

public class NetworkExceptionDelayPeerDecorator: IPeer
{
    private readonly IPeer _peer;
    private readonly TimeSpan _delay;

    public NetworkExceptionDelayPeerDecorator(IPeer peer, TimeSpan delay)
    {
        _peer = peer;
        _delay = delay;
    }

    public PeerId Id =>
        _peer.Id;

    public async Task<HeartbeatResponse?> SendHeartbeat(HeartbeatRequest request, CancellationToken token)
    {
        var response = await _peer.SendHeartbeat(request, token);
        if (response is null)
        {
            await Task.Delay(_delay, token);
        }
        return response;
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        var response = await _peer.SendRequestVote(request, token);
        if (response is null)
        {
            await Task.Delay(_delay, token);
        }
        return response;
    }

    public async Task SendAppendEntries(CancellationToken token)
    {
        await _peer.SendAppendEntries(token);
    }
}