using Consensus.Core.Submit;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using Consensus.Raft.State;
using TaskFlux.Models;
using TaskFlux.Node;

namespace TaskFlux.Host.Infrastructure;

public class InfoUpdaterRaftConsensusModuleDecorator<TCommand, TResult> : IRaftConsensusModule<TCommand, TResult>
{
    private readonly IRaftConsensusModule<TCommand, TResult> _module;
    private readonly ClusterInfo _clusterInfo;
    private readonly NodeInfo _nodeInfo;

    public InfoUpdaterRaftConsensusModuleDecorator(IRaftConsensusModule<TCommand, TResult> module,
                                                   ClusterInfo clusterInfo,
                                                   NodeInfo nodeInfo)
    {
        _module = module;
        _clusterInfo = clusterInfo;
        _nodeInfo = nodeInfo;
        _module.RoleChanged += OnRoleChanged;
    }

    private void OnRoleChanged(NodeRole oldRole, NodeRole newRole)
    {
        if (newRole == NodeRole.Leader)
        {
            _clusterInfo.LeaderId = _module.Id;
        }

        _nodeInfo.CurrentRole = newRole;
    }

    public NodeId Id =>
        _module.Id;

    public NodeRole CurrentRole =>
        _module.CurrentRole;

    public Term CurrentTerm =>
        _module.CurrentTerm;

    public NodeId? VotedFor =>
        _module.VotedFor;

    public bool TryUpdateState(State<TCommand, TResult> newState,
                               State<TCommand, TResult> oldState)
    {
        return _module.TryUpdateState(newState, oldState);
    }

    public IBackgroundJobQueue BackgroundJobQueue =>
        _module.BackgroundJobQueue;

    public StoragePersistenceFacade PersistenceFacade =>
        _module.PersistenceFacade;

    public PeerGroup PeerGroup =>
        _module.PeerGroup;

    public IApplication<TCommand, TResult> Application
    {
        get => _module.Application;
        set => _module.Application = value;
    }

    public RequestVoteResponse Handle(RequestVoteRequest request)
    {
        return _module.Handle(request);
    }

    public AppendEntriesResponse Handle(AppendEntriesRequest request)
    {
        var response = _module.Handle(request);
        if (response.Success)
        {
            _clusterInfo.LeaderId = request.LeaderId;
        }

        return response;
    }

    public IEnumerable<InstallSnapshotResponse> Handle(InstallSnapshotRequest request, CancellationToken token)
    {
        return _module.Handle(request, token);
    }

    public SubmitResponse<TResult> Handle(TCommand command)
    {
        return _module.Handle(command);
    }

    public event RoleChangedEventHandler? RoleChanged
    {
        add => _module.RoleChanged += value;
        remove => _module.RoleChanged -= value;
    }

    public State<TCommand, TResult> CreateFollowerState()
    {
        return _module.CreateFollowerState();
    }

    public State<TCommand, TResult> CreateLeaderState()
    {
        return _module.CreateLeaderState();
    }

    public State<TCommand, TResult> CreateCandidateState()
    {
        return _module.CreateCandidateState();
    }

    public SubmitResponse<TResult> Handle(TCommand command, CancellationToken token)
    {
        return _module.Handle(command, token);
    }
}