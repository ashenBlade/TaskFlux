using Raft.Core.Commands;
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

                    lock (UpdateLocker)
                    {
                        if (StateMachine.CurrentState != this)
                        {
                            _logger.Debug("Найден узел с более высоким термом, но после получения блокировки состояние уже было изменено");
                            return;
                        }
                        
                        Node.CurrentTerm = response.CurrentTerm;
                        StateMachine.CurrentState = FollowerState.Create(StateMachine);
                        StateMachine.ElectionTimer.Start();
                        StateMachine.Node.VotedFor = null;
                    }
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
        lock (UpdateLocker)
        {
            if (StateMachine.CurrentState != this)
            {
                _logger.Debug("Перейти в Leader не получилось - после получения блокировки состояние уже изменилось");
                return;
            }
            StateMachine.CurrentState = LeaderState.Create(StateMachine);
            StateMachine.HeartbeatTimer.Start();
            StateMachine.ElectionTimer.Stop();
        }
        
        bool QuorumReached()
        {
            return Node.PeerGroup.IsQuorumReached(votes);
        }
    }

    // ReSharper disable once ArrangeStaticMemberQualifier
    private void OnElectionTimerTimeout()
    {
        // ReSharper disable once InconsistentlySynchronizedField
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        lock (UpdateLocker)
        {
            if (StateMachine.CurrentState != this)
            {
                return;
            }
            
            _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");
            var node = Node;
            node.CurrentTerm = node.CurrentTerm.Increment();
            node.VotedFor = node.Id;
            StateMachine.CurrentState = CandidateState.Create(StateMachine);
        }
    }

    public override void Dispose()
    {
        _cts?.Dispose();
        StateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        base.Dispose();
    }

    internal static CandidateState Create(IStateMachine stateMachine)
    {
        return new CandidateState(stateMachine, stateMachine.Logger.ForContext("SourceContext", "Candidate"));
    }
}