using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;
using Serilog;

namespace Raft.Core.Node.LeaderState;

internal class LeaderState: BaseNodeState
{
    public override NodeRole Role => NodeRole.Leader;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly PeerProcessor[] _processors;

    internal LeaderState(INode node, ILogger logger, IRequestQueueFactory queueFactory)
        : base(node)
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
        _logger.Verbose("Сработал Heartbeat таймер. Отправляю команду всем обработчикам узлов");
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
            CurrentState = FollowerState.Create(Node);
            ElectionTimer.Start();
            CurrentTerm = request.Term;
            VotedFor = null;
        }
        
        if (Log.Contains(request.PrevLogEntryInfo) is false)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }
        
        if (0 < request.Entries.Count)
        {
            // Записи могут перекрываться. (например, новый лидер затирает старые записи)
            // Поэтому необходимо найти индекс,
            // начиная с которого необходимо добавить в лог новые записи.

            // Индекс расхождения в нашем логе
            for (int logIndex = request.PrevLogEntryInfo.Index + 1, 
                     
                     // Соответвующий индекс в массиве новых элементов
                     newEntriesIndex = 0; 
                 
                 logIndex < Log.Entries.Count && 
                 newEntriesIndex < request.Entries.Count; 
                 
                 logIndex++,
                 newEntriesIndex++)
            {
                if (Log.Entries[logIndex].Term == request.Entries[newEntriesIndex].Term) 
                    continue;
                
                // Может случиться так, что все присланные вхождения уже есть в нашем логе
                if (newEntriesIndex < request.Entries.Count)
                {
                    Log.AppendUpdateRange(request.Entries.Skip(newEntriesIndex), logIndex);
                }
                break;
            }
        }

        if (Log.CommitIndex < request.LeaderCommit)
        {
            Log.Commit(Math.Min(request.LeaderCommit, Log.LastEntry.Index));
        }


        return AppendEntriesResponse.Ok(CurrentTerm);
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            _logger.Debug("Получен RequestVote с большим термом {MyTerm} < {NewTerm}. Перехожу в Follower", CurrentTerm, request.CandidateTerm);
            CurrentTerm = request.CandidateTerm;
            VotedFor = request.CandidateId;
            CurrentState = FollowerState.Create(Node);
            ElectionTimer.Start();

            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
        }
        
        var canVote = 
            // Ранее не голосовали
            VotedFor is null || 
            // Текущий лидер/кандидат посылает этот запрос (почему бы не согласиться)
            VotedFor == request.CandidateId;
        
        // Отдать свободный голос можем только за кандидата 
        if (canVote && 
            // У которого лог в консистентном с нашим состоянием
            Log.Contains(request.LastLogEntryInfo))
        {
            CurrentState = FollowerState.Create(Node);
            ElectionTimer.Start();
            CurrentTerm = request.CandidateTerm;
            VotedFor = request.CandidateId;
            
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

    public static LeaderState Create(INode node)
    {
        return new LeaderState(node, node.Logger.ForContext("SourceContext", "Leader"), new ChannelRequestQueueFactory(node.Log));
    }

    public override SubmitResponse Apply(SubmitRequest request)
    {
        // Добавляем команду в лог

        var appended = Log.Append(new LogEntry( CurrentTerm, request.Command ));
        // Сигнализируем узлам, чтобы принялись за работу
        var synchronizer = new AppendEntriesRequestSynchronizer(PeerGroup, appended.Index);
        Array.ForEach(_processors, p => p.NotifyAppendEntries(synchronizer));
        
        // Ждем достижения кворума
        synchronizer.LogReplicated.Wait(_cts.Token);
        
        // Пытаемся применить команду к машине состояний
        StateMachine.Submit(request.Command);
        
        // Обновляем индекс последней закоммиченной записи
        Log.Commit(appended.Index);
        
        // Возвращаем результат
        return new SubmitResponse(new LogEntry(appended.Term, request.Command));
    }
}