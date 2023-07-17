using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Commands.Submit;
using Consensus.Core.Log;
using Serilog;

namespace Consensus.Core.State.LeaderState;

internal class LeaderState: BaseConsensusModuleState
{
    public override NodeRole Role => NodeRole.Leader;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly PeerProcessor[] _processors;

    internal LeaderState(IConsensusModule consensusModule, ILogger logger, IRequestQueueFactory queueFactory)
        : base(consensusModule)
    {
        _logger = logger;
        _processors = CreatePeerProcessors(this, queueFactory);
        JobQueue.EnqueueInfinite(ProcessPeersAsync, _cts.Token);
        HeartbeatTimer.Timeout += OnHeartbeatTimer;
    }

    private static PeerProcessor[] CreatePeerProcessors(LeaderState state, IRequestQueueFactory queueFactory)
    {
        var peers = state.PeerGroup.Peers;
        var processors = new PeerProcessor[peers.Count];
        for (var i = 0; i < peers.Count; i++)
        {
            processors[i] = new PeerProcessor(state, peers[i], queueFactory.CreateQueue());
        }

        return processors;
    }
    
    private void OnHeartbeatTimer()
    {
        Array.ForEach(_processors, static p => p.NotifyHeartbeatTimeout());
        HeartbeatTimer.Start();
    }

    private async Task ProcessPeersAsync()
    {
        _logger.Verbose("Запускаю обработчиков узлов");
        var tasks = _processors.Select(x => x.StartServingAsync(_cts.Token));
        Array.ForEach(_processors, static p => p.NotifyHeartbeatTimeout());
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
            when (_cts.Token.IsCancellationRequested)
        { }
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < CurrentTerm)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        if (CurrentTerm < request.Term)
        {
            CurrentState = FollowerState.Create(ConsensusModule);
            ElectionTimer.Start();
            ConsensusModule.UpdateState(request.Term, null);
        }
        
        if (Log.Contains(request.PrevLogEntryInfo) is false)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }
        
        if (0 < request.Entries.Count)
        {
            Log.InsertRange(request.Entries, request.PrevLogEntryInfo.Index + 1);
        }

        if (Log.CommitIndex < request.LeaderCommit)
        {
            Log.Commit(Math.Min(request.LeaderCommit, Log.LastEntry.Index));
            Log.ApplyCommitted(StateMachine);
        }

        return AppendEntriesResponse.Ok(CurrentTerm);
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            ConsensusModule.UpdateState(request.CandidateTerm, request.CandidateId);
            CurrentState = FollowerState.Create(ConsensusModule);
            ElectionTimer.Start();

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }
        
        var canVote = 
            // Ранее не голосовали
            VotedFor is null || 
            // В этом терме мы за него уже проголосовали
            VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата 
        if (canVote &&                              // За которого можем проголосовать и
            !Log.Conflicts(request.LastLogEntryInfo)) // У которого лог не хуже нашего
        {
            ConsensusModule.UpdateState(request.CandidateTerm, request.CandidateId);
            CurrentState = FollowerState.Create(ConsensusModule);
            ElectionTimer.Start();
            
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

    public static LeaderState Create(IConsensusModule consensusModule)
    {
        return new LeaderState(consensusModule, consensusModule.Logger.ForContext("SourceContext", "Leader"), new ChannelRequestQueueFactory(consensusModule.Log));
    }

    public override SubmitResponse Apply(SubmitRequest request)
    {
        // Добавляем команду в лог
        var entry = new LogEntry( CurrentTerm, request.Command );
        var appended = Log.Append(entry);
        
        // Сигнализируем узлам, чтобы принялись реплицировать
        var synchronizer = new AppendEntriesRequestSynchronizer(PeerGroup, appended.Index);
        Array.ForEach(_processors, p => p.NotifyAppendEntries(synchronizer));
        
        // Ждем достижения кворума
        synchronizer.LogReplicated.Wait(_cts.Token);
        
        // Применяем команду к машине состояний
        var response = StateMachine.Apply(request.Command);
        
        // Обновляем индекс последней закоммиченной записи
        Log.Commit(appended.Index);
        Log.SetLastApplied(appended.Index);
        
        // Возвращаем результат
        return SubmitResponse.Success(response);
    }
}