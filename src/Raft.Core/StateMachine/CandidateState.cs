using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
using Serilog;

namespace Raft.Core.StateMachine;

internal class CandidateState: BaseState
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

    private async Task<RequestVoteResponse?[]> SendRequestVotes(List<IPeer> peers)
    {
        // Отправляем запрос всем пирам
        var request = new RequestVoteRequest()
        {
            CandidateId = StateMachine.Node.Id,
            CandidateTerm = StateMachine.Node.CurrentTerm,
            LastLog = StateMachine.Log.LastLogEntry
        };

        var requests = new Task<RequestVoteResponse?>[peers.Count];
        for (var i = 0; i < peers.Count; i++)
        {
            requests[i] = peers[i].SendRequestVote(request, _cts.Token);
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
            await RunQuorumInner();
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

    internal async Task RunQuorumInner()
    {
        _logger.Debug("Запускаю кворум для получения большинства голосов");
        var peers = new List<IPeer>(Node.PeerGroup.Peers.Count);
        var term = Node.CurrentTerm;
        peers.AddRange(Node.PeerGroup.Peers);
        // Держим в уме узлы, которые не ответили (вообще не ответили)
        var swap = new List<IPeer>();
        var votes = 0;
        _logger.Debug("Начинаю раунд кворума для терма {Term}. Отправляю запросы на узлы: {Peers}", term, peers.Select(x => x.Id));
        while (!QuorumReached())
        {
            var responses = await SendRequestVotes(peers);
            _logger.Debug("От узлов вернулись ответы");
            if (_cts.Token.IsCancellationRequested)
            {
                _logger.Debug("Операция была отменена. Завершаю кворум");
                return;
            }
            for (var i = 0; i < responses.Length; i++)
            {
                var response = responses[i];
                // Узел не ответил
                if (response is null)
                {
                    swap.Add(peers[i]);
                    _logger.Verbose("Узел {NodeId} не вернул ответ", peers[i].Id);
                }
                // Узел отдал голос
                else if (response.VoteGranted)
                {
                    votes++;
                    _logger.Verbose("Узел {NodeId} отдал голос за", peers[i].Id);
                }
                // Узел имеет более высокий Term
                else if (Node.CurrentTerm < response.CurrentTerm)
                {
                    _logger.Verbose("Узел {NodeId} имеет более высокий Term. Перехожу в состояние Follower", peers[i].Id);
                    _cts.Cancel();
                    
                    Node.CurrentTerm = response.CurrentTerm;
                    StateMachine.CurrentState = FollowerState.Start(StateMachine);
                    return;
                }
                // Узел отказался по другой причине.
                // Возможно он другой кандидат
                else
                {
                    _logger.Verbose("Узел {NodeId} не отдал голос за", peers[i].Id);
                }
            }
            
            ( peers, swap ) = ( swap, peers );
            swap.Clear();
            
            // Больше не кому слать
            if (peers.Count == 0)
            {
                if (QuorumReached())
                {
                    break;
                }

                _logger.Debug("Кворум не достигнут и нет узлов, которым можно послать запросы. Дожидаюсь завершения Election Timeout");
                return;
            }
        }
        
        _logger.Debug("Кворум собран. Получено {VotesCount} голосов. Перехожу в состояние Leader", votes);
        
        _cts.Token.ThrowIfCancellationRequested();
        StateMachine.CurrentState = LeaderState.Start(StateMachine);
        StateMachine.ElectionTimer.Stop();
        
        bool QuorumReached()
        {
            return Node.PeerGroup.IsQuorumReached(votes);
        }
    }


    public override async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        return await base.Apply(request, token);
    }

    public override async Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        return await base.Apply(request, token);
    }

    private void OnElectionTimerTimeout()
    {
        _cts.Cancel();
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");
        StateMachine.CurrentState = Start(StateMachine);
    }

    public override void Dispose()
    {
        _cts?.Dispose();
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        base.Dispose();
    }

    internal static CandidateState Start(IStateMachine stateMachine)
    {
        // При входе в кандидата, номер терма увеличивается
        var node = stateMachine.Node;
        node.CurrentTerm = node.CurrentTerm.Increment();
        node.VotedFor = node.Id;

        var state = new CandidateState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Candidate"));
        stateMachine.ElectionTimer.Start();
        return state;
    }
}