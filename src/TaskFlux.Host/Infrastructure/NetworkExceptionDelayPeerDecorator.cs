using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using TaskFlux.Core;

namespace TaskFlux.Host.Infrastructure;

/// <summary>
/// Используется при тестах на одной машине, чтобы не грузить бесконечными запросами соединения
/// </summary>
public class NetworkExceptionDelayPeerDecorator : IPeer
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

    public InstallSnapshotResponse? SendInstallSnapshot(InstallSnapshotRequest request, CancellationToken token)
    {
        var response = _peer.SendInstallSnapshot(request, token);
        if (response is null)
        {
            Thread.Sleep(_delay);
        }

        return response;
    }
}