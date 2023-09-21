using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Consensus.Raft.State;
using TaskFlux.Core;
using TaskFlux.Node;

namespace TaskFlux.Host.Infrastructure;

public class InfoUpdaterConsensusModuleDecorator<TCommand, TResult> : IConsensusModule<TCommand, TResult>
{
    private readonly IConsensusModule<TCommand, TResult> _module;
    private readonly ClusterInfo _clusterInfo;
    private readonly NodeInfo _nodeInfo;

    public InfoUpdaterConsensusModuleDecorator(IConsensusModule<TCommand, TResult> module,
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

    public Term CurrentTerm =>
        _module.CurrentTerm;

    public NodeId? VotedFor =>
        _module.VotedFor;

    public State<TCommand, TResult> CurrentState => _module.CurrentState;

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

    public SubmitResponse<TResult> Handle(SubmitRequest<TCommand> request)
    {
        return _module.Handle(request);
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
}