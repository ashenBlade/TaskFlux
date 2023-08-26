using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Consensus.Raft.Persistence;
using Serilog;
using TaskFlux.Core;

namespace Consensus.Raft.State.LeaderState;

public class LeaderState<TCommand, TResponse>: State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Leader;
    private readonly ILogger _logger;
    private readonly ICommandSerializer<TCommand> _commandSerializer;
    private readonly CancellationTokenSource _cts = new();
    private readonly PeerProcessor<TCommand, TResponse>[] _processors;

    internal LeaderState(IConsensusModule<TCommand, TResponse> consensusModule, 
                         ILogger logger, 
                         ICommandSerializer<TCommand> commandSerializer,
                         IRequestQueueFactory queueFactory)
        : base(consensusModule)
    {
        _logger = logger;
        _commandSerializer = commandSerializer;
        _processors = CreatePeerProcessors(this, queueFactory);
    }
    
    public override void Initialize()
    {
        BackgroundJobQueue.RunInfinite(StartPeersAsync, _cts.Token);
        HeartbeatTimer.Timeout += OnHeartbeatTimer;
    }

    private static PeerProcessor<TCommand, TResponse>[] CreatePeerProcessors(LeaderState<TCommand, TResponse> state, IRequestQueueFactory queueFactory)
    {
        var peers = state.PeerGroup.Peers;
        var processors = new PeerProcessor<TCommand, TResponse>[peers.Count];
        for (var i = 0; i < peers.Count; i++)
        {
            processors[i] = new PeerProcessor<TCommand, TResponse>(state, peers[i], queueFactory.CreateQueue());
        }

        return processors;
    }
    
    private void OnHeartbeatTimer()
    {
        Array.ForEach(_processors, static p => p.NotifyHeartbeatTimeout());
        HeartbeatTimer.Start();
    }

    private async Task StartPeersAsync()
    {
        _logger.Verbose("Запускаю обработчиков узлов");
        var tasks = _processors.Select(x => x.StartServingAsync(_cts.Token));
        
        // Сразу отправляем всем Heartbeat, чтобы уведомить о том, что мы новый лидер
        Array.ForEach(_processors, static p => p.NotifyHeartbeatTimeout());
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
            when (_cts.Token.IsCancellationRequested)
        { }
        catch (Exception unhandled)
        {
            _logger.Fatal(unhandled, "Во время работы обработчиков узлов возникло необработанное исключение");
            throw;
        }
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term <= CurrentTerm)
        {
            // Согласно алгоритму, в каждом терме свой лидер, поэтому ситуации равенства быть не должно
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        var follower = ConsensusModule.CreateFollowerState();
        ConsensusModule.TryUpdateState(follower, this);
        return ConsensusModule.Handle(request);
    }
    
    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            var followerState = ConsensusModule.CreateFollowerState();
            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);
                ElectionTimer.Start();
            }

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }
        
        var canVote = 
            // Ранее не голосовали
            VotedFor is null || 
            // В этом терме мы за него уже проголосовали
            VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата 
        if (canVote &&                                              // За которого можем проголосовать и
            !PersistenceFacade.Conflicts(request.LastLogEntryInfo)) // У которого лог не хуже нашего
        {
            var followerState = ConsensusModule.CreateFollowerState();
            if (ConsensusModule.TryUpdateState(followerState, this))
            {
                ConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, request.CandidateId);
                ElectionTimer.Start();
            }
            
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override void Dispose()
    {
        // CommandQueue.Enqueue(new StopHeartbeatTimerCommand(this, Node));
        HeartbeatTimer.Stop();
        HeartbeatTimer.Timeout -= OnHeartbeatTimer;
    }

    public override InstallSnapshotResponse Apply(InstallSnapshotRequest request, CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            return new InstallSnapshotResponse(CurrentTerm);
        }
        
        var followerState = ConsensusModule.CreateFollowerState();
        if (ConsensusModule.TryUpdateState(followerState, this))
        {
            return ConsensusModule.Handle(request, token);
        }

        return new InstallSnapshotResponse(CurrentTerm);
    }

    public override SubmitResponse<TResponse> Apply(SubmitRequest<TCommand> request)
    {
        if (request.Descriptor.IsReadonly)
        {
            // Короткий путь для readonly команд
            return SubmitResponse<TResponse>.Success( StateMachine.Apply(request.Descriptor.Command), true );
        }
        
        // Добавляем команду в лог
        var entry = new LogEntry( CurrentTerm, _commandSerializer.Serialize(request.Descriptor.Command) );
        var appended = PersistenceFacade.Append(entry);
        
        // Сигнализируем узлам, чтобы принялись реплицировать
        var synchronizer = new AppendEntriesRequestSynchronizer(PeerGroup, appended.Index);
        Array.ForEach(_processors, p => p.NotifyAppendEntries(synchronizer));
        
        // Ждем достижения кворума
        // TODO: убрать асинхронность
        synchronizer.LogReplicated.Wait(_cts.Token);
        
        // Применяем команду к машине состояний
        var response = StateMachine.Apply(request.Descriptor.Command);
        
        // Обновляем индекс последней закоммиченной записи
        PersistenceFacade.Commit(appended.Index);
        PersistenceFacade.SetLastApplied(appended.Index);

        
        if (MaxLogFileSize < PersistenceFacade.LogFileSize)
        {
            var snapshot = StateMachine.GetSnapshot();
            var snapshotLastEntryInfo = PersistenceFacade.LastApplied;
            PersistenceFacade.SaveSnapshot(snapshotLastEntryInfo, snapshot, CancellationToken.None);
            PersistenceFacade.ClearCommandLog();
        }
        
        // Возвращаем результат
        return SubmitResponse<TResponse>.Success(response, true);
    }
}