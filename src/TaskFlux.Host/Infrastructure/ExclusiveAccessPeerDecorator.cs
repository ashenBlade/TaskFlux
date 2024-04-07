using TaskFlux.Consensus;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Commands.InstallSnapshot;
using TaskFlux.Consensus.Commands.RequestVote;
using TaskFlux.Core;

namespace TaskFlux.Host.Infrastructure;

public class ExclusiveAccessPeerDecorator : IPeer
{
    private readonly IPeer _peer;
    private object _locker = new();

    public ExclusiveAccessPeerDecorator(IPeer peer)
    {
        _peer = peer;
    }

    public NodeId Id => _peer.Id;

    public AppendEntriesResponse SendAppendEntries(AppendEntriesRequest request, CancellationToken token)
    {
        lock (_locker)
        {
            return _peer.SendAppendEntries(request, token);
        }
    }

    public RequestVoteResponse SendRequestVote(RequestVoteRequest request, CancellationToken token)
    {
        lock (_locker)
        {
            return _peer.SendRequestVote(request, token);
        }
    }

    public InstallSnapshotResponse SendInstallSnapshot(InstallSnapshotRequest request, CancellationToken token)
    {
        lock (_locker)
        {
            return _peer.SendInstallSnapshot(request, token);
        }
    }
}