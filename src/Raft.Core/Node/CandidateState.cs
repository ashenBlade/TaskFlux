using Raft.Core.Commands;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Commands.Submit;
using Serilog;

namespace Raft.Core.Node;

internal class CandidateState: BaseNodeState
{
    public override NodeRole Role => NodeRole.Candidate;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    internal CandidateState(INode node, ILogger logger)
        :base(node)
    {
        _logger = logger;
        _cts = new();
        ElectionTimer.Timeout += OnElectionTimerTimeout;
        JobQueue.EnqueueInfinite(RunQuorum, _cts.Token);
    }

    private async Task<RequestVoteResponse?[]> SendRequestVotes(List<IPeer> peers, CancellationToken token)
    {
        // Отправляем запрос всем пирам
        var request = new RequestVoteRequest(CandidateId: Id,
            CandidateTerm: CurrentTerm, LastLogEntryInfo: Log.LastEntry);

        var requests = new Task<RequestVoteResponse?>[peers.Count];
        for (var i = 0; i < peers.Count; i++)
        {
            requests[i] = peers[i].SendRequestVote(request, token);
        }

        return await Task.WhenAll( requests );
    }

    /// <summary>
    /// Запустить раунды кворума и попытаться получить большинство голосов.
    /// Выполняется в фоновом потоке
    /// </summary>
    /// <remarks>
    /// Дополнительные раунды нужны, когда какой-то узел не отдал свой голос.
    /// Всем отправившим ответ узлам (отдавшим голос или нет) запросы больше не посылаем.
    /// Грубо говоря, этот метод работает пока все узлы не ответят
    /// </remarks>
    internal async Task RunQuorum()
    {
        try
        {
            await RunQuorumInner(_cts.Token);
        }
        catch (TaskCanceledException)
        {
            _logger.Debug("Сбор кворума прерван - задача отменена");
        }
        catch (ObjectDisposedException)
        {
            _logger.Verbose("Источник токенов удален во время отправки запросов");
        }
    }

    internal async Task RunQuorumInner(CancellationToken token)
    {
        _logger.Debug("Запускаю кворум для получения большинства голосов");
        var leftPeers = new List<IPeer>(PeerGroup.Peers.Count);
        var term = CurrentTerm;
        leftPeers.AddRange(PeerGroup.Peers);
        
        var notResponded = new List<IPeer>();
        var votes = 0;
        _logger.Debug("Начинаю раунд кворума для терма {Term}. Отправляю запросы на узлы: {Peers}", term, leftPeers.Select(x => x.Id));
        while (!QuorumReached())
        {
            var responses = await SendRequestVotes(leftPeers, token);
            if (token.IsCancellationRequested)
            {
                _logger.Debug("Операция была отменена во время отправки запросов. Завершаю кворум");
                return;
            }
            for (var i = 0; i < responses.Length; i++)
            {
                var response = responses[i];
                if (response is null)
                {
                    notResponded.Add(leftPeers[i]);
                    _logger.Verbose("Узел {NodeId} не вернул ответ", leftPeers[i].Id);
                }
                else if (response.VoteGranted)
                {
                    votes++;
                    _logger.Verbose("Узел {NodeId} отдал голос за", leftPeers[i].Id);
                }
                else if (CurrentTerm < response.CurrentTerm)
                {
                    _logger.Verbose("Узел {NodeId} имеет более высокий Term. Перехожу в состояние Follower", leftPeers[i].Id);
                    _cts.Cancel();

                    CommandQueue.Enqueue(new MoveToFollowerStateCommand(response.CurrentTerm, null, this, Node));
                    return;
                }
                else
                {
                    _logger.Verbose("Узел {NodeId} не отдал голос за", leftPeers[i].Id);
                }
            }
            
            ( leftPeers, notResponded ) = ( notResponded, leftPeers );
            notResponded.Clear();
            
            if (leftPeers.Count == 0 && !QuorumReached())
            {
                _logger.Debug("Кворум не достигнут и нет узлов, которым можно послать запросы. Дожидаюсь завершения Election Timeout");
                return;
            }
        }
        
        _logger.Debug("Кворум собран. Получено {VotesCount} голосов. Посылаю команду перехода в состояние Leader", votes);

        if (token.IsCancellationRequested)
        {
            _logger.Debug("Токен был отменен. Команду перехода в Leader не посылаю");
            return;
        }

        CommandQueue.Enqueue(new MoveToLeaderStateCommand(this, Node));
        
        bool QuorumReached()
        {
            return PeerGroup.IsQuorumReached(votes);
        }
    }

    private void OnElectionTimerTimeout()
    {
        ElectionTimer.Timeout -= OnElectionTimerTimeout;

        _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");

        CommandQueue.Enqueue(new MoveToCandidateAfterElectionTimerTimeoutCommand(this, Node));
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

    public override SubmitResponse Apply(SubmitRequest request)
    {
        throw new InvalidOperationException("Текущий узел в состоянии Candidate");
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
        try
        {
            _cts.Cancel();
            _cts.Dispose();
        }
        catch (ObjectDisposedException)
        { }
        
        ElectionTimer.Timeout -= OnElectionTimerTimeout;
    }

    internal static CandidateState Create(INode node)
    {
        return new CandidateState(node, node.Logger.ForContext("SourceContext", "Candidate"));
    }
}