using Consensus.Core;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.RequestVote;

namespace Consensus.Peer.Decorators;

public class ExclusiveAccessPeerDecorator: IPeer
{
    private readonly IPeer _peer;
    private readonly SemaphoreSlim _sem = new(1);
    
    public ExclusiveAccessPeerDecorator(IPeer peer)
    {
        _peer = peer;
    }

    public NodeId Id => _peer.Id;

    public async Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        await _sem.WaitAsync(token);
        try
        {
            return await _peer.SendAppendEntries(request, token);
        }
        finally
        {
            _sem.Release();
        }
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        await _sem.WaitAsync(token);
        try
        {
            return await _peer.SendRequestVote(request, token);
        }
        finally
        {
            _sem.Release();
        }
    }
}