using Raft.Core.Commands;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Peer;
using Serilog;

namespace Raft.Core.StateMachine;

internal class CandidateState: NodeState
{
    public override NodeRole Role => NodeRole.Candidate;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    internal CandidateState(IStateMachine stateMachine, ILogger logger)
        :base(stateMachine)
    {
        _logger = logger;
        _cts = new();
        StateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
        StateMachine.JobQueue.EnqueueInfinite(RunQuorum, _cts.Token);
    }

    private async Task<RequestVoteResponse?[]> SendRequestVotes(List<IPeer> peers, CancellationToken token)
    {
        // Отправляем запрос всем пирам
        var request = new RequestVoteRequest(CandidateId: StateMachine.Node.Id,
            CandidateTerm: StateMachine.Node.CurrentTerm, LastLog: StateMachine.Log.LastLogEntry);

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
        var leftPeers = new List<IPeer>(Node.PeerGroup.Peers.Count);
        var term = Node.CurrentTerm;
        leftPeers.AddRange(Node.PeerGroup.Peers);
        
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
                else if (Node.CurrentTerm < response.CurrentTerm)
                {
                    _logger.Verbose("Узел {NodeId} имеет более высокий Term. Перехожу в состояние Follower", leftPeers[i].Id);
                    _cts.Cancel();

                    StateMachine.CommandQueue.Enqueue(new MoveToFollowerStateCommand(response.CurrentTerm, null, this, StateMachine));
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

        StateMachine.CommandQueue.Enqueue(new MoveToLeaderStateCommand(this, StateMachine));
        
        bool QuorumReached()
        {
            return Node.PeerGroup.IsQuorumReached(votes);
        }
    }

    private void OnElectionTimerTimeout()
    {
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;

        _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");

        StateMachine.CommandQueue.Enqueue(new MoveToCandidateAfterElectionTimerTimeoutCommand(this, StateMachine));
    }

    public override void Dispose()
    {
        try
        {
            _cts.Cancel();
            _cts?.Dispose();
        }
        catch (ObjectDisposedException)
        { }
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        base.Dispose();
    }

    internal static CandidateState Create(IStateMachine stateMachine)
    {
        return new CandidateState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Candidate"));
    }
}