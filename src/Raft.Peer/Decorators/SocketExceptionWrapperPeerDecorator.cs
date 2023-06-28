using System.Net.Sockets;
using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;

namespace Raft.Peer.Decorators;

public class SocketExceptionWrapperPeerDecorator: IPeer
{
    private readonly IPeer _peer;

    public SocketExceptionWrapperPeerDecorator(IPeer peer)
    {
        _peer = peer;
    }

    public NodeId Id => _peer.Id;

    public async Task<AppendEntriesResponse?> SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        try
        {
            return await _peer.SendAppendEntries(request, token);
        }
        catch (SocketException)
        {
            return null;
        }
    }

    public async Task<RequestVoteResponse?> SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        try
        {
            return await _peer.SendRequestVote(request, token);
        }
        catch (SocketException)
        {
            return null;
        }
    }
}