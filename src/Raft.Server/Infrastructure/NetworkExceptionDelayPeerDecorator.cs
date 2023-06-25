using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;

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

    public NodeId Id =>
        _peer.Id;

    public async Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        var response = await _peer.SendAppendEntries(request, token);
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
}