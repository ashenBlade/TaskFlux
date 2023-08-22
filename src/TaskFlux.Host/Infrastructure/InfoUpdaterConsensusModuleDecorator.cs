using Consensus.CommandQueue;
using Consensus.Core;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.InstallSnapshot;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;
using Consensus.Core.Log;
using Consensus.Core.State;
using Consensus.Core.State.LeaderState;
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

    public ConsensusModuleState<TCommand, TResult> CurrentState
    {
        get => _module.CurrentState;
        set => _module.CurrentState = value;
    }

    public ITimer ElectionTimer =>
        _module.ElectionTimer;

    public ITimer HeartbeatTimer =>
        _module.HeartbeatTimer;

    public IBackgroundJobQueue BackgroundJobQueue =>
        _module.BackgroundJobQueue;

    public ICommandQueue CommandQueue =>
        _module.CommandQueue;

    public ILog Log =>
        _module.Log;

    public PeerGroup PeerGroup =>
        _module.PeerGroup;

    public IStateMachine<TCommand, TResult> StateMachine =>
        _module.StateMachine;

    public IMetadataStorage MetadataStorage =>
        _module.MetadataStorage;

    public ISerializer<TCommand> CommandSerializer =>
        _module.CommandSerializer;

    public void UpdateState(Term newTerm, NodeId? votedFor)
    {
        _module.UpdateState(newTerm, votedFor);
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

    public InstallSnapshotResponse Handle(InstallSnapshotRequest request)
    {
        return _module.Handle(request);
    }

    public event RoleChangedEventHandler? RoleChanged
    {
        add => _module.RoleChanged += value;
        remove => _module.RoleChanged -= value;
    }

    public FollowerState<TCommand, TResult> CreateFollowerState()
    {
        return _module.CreateFollowerState();
    }

    public LeaderState<TCommand, TResult> CreateLeaderState()
    {
        return _module.CreateLeaderState();
    }

    public CandidateState<TCommand, TResult> CreateCandidateState()
    {
        return _module.CreateCandidateState();
    }
}