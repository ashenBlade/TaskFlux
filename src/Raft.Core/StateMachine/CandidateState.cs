using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
using Serilog;

namespace Raft.Core.StateMachine;

public class CandidateState: INodeState
{
    public NodeRole Role => NodeRole.Candidate;
    private readonly IStateMachine _stateMachine;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private INode Node => _stateMachine.Node;

    private CandidateState(IStateMachine stateMachine, ILogger logger)
    {
        _stateMachine = stateMachine;
        _logger = logger;
        _cts = new();
        _stateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
        _stateMachine.JobQueue.EnqueueInfinite(RunQuorum, _cts.Token);
    }

    private async Task<RequestVoteResponse?[]> RunQuorumRound(List<IPeer> peers, CancellationToken token)
    {
        // Отправляем запрос всем пирам
        var request = new RequestVoteRequest()
        {
            CandidateId = _stateMachine.Node.Id,
            CandidateTerm = _stateMachine.Node.CurrentTerm,
            LastLog = _stateMachine.Log.LastLogEntry
        };

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
    internal async Task RunQuorum()
    {
        _logger.Debug("Запускаю кворум для получения большинства голосов");
        var peers = new List<IPeer>(Node.PeerGroup.Peers.Count);
        var term = Node.CurrentTerm;
        peers.AddRange(Node.PeerGroup.Peers);
        // Держим в уме узлы, которые не ответили взаимностью
        var swap = new List<IPeer>();
        var votes = 0;
        
        var token = _cts.Token;
        _logger.Debug("Начинаю раунд кворума для терма {Term}. Отправляю запросы на узлы: {Peers}", term, peers.Select(x => x.Id));
        while (!QuorumReached())
        {
            var responses = await RunQuorumRound(peers, token);
            _logger.Debug("От узлов вернулись ответы");
            if (token.IsCancellationRequested)
            {
                _logger.Debug("Токен был отменен. Завершаю кворум");
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
                // Узел согласился
                else if (response.VoteGranted)
                {
                    votes++;
                    _logger.Verbose("Узел {NodeId} отдал голос за", peers[i].Id);
                }
                // Узел имеет более высокий Term
                else if (Node.CurrentTerm < response.CurrentTerm)
                {
                    swap.Add(peers[i]);
                    _logger.Verbose("Узел {NodeId} имеет более высокий Term. Перехожу в состояние Follower", peers[i].Id);
                    _cts.Cancel();
                    
                    Node.CurrentTerm = response.CurrentTerm;
                    var state = FollowerState.Start(_stateMachine);
                    _stateMachine.CurrentState = state;
                    return;
                }
                // Узел отказался по другой причине. Возможно он кандидат
                else
                {
                    swap.Add(peers[i]);
                    _logger.Verbose("Узел {NodeId} не отдал голос за", peers[i].Id);
                }
            }
            
            ( peers, swap ) = ( swap, peers );
            swap.Clear();
            
            if (peers.Count == 0)
            {
                if (QuorumReached())
                {
                    break;
                }

                _logger.Debug("Кворум не достигнут и нет узлов, которым можно послать запросы. ");
                break;
            }
        }
        
        _logger.Debug("Кворум собран. Получено {VotesCount} голосов. Перехожу в состояние Leader", votes);
        
        _stateMachine.CurrentState = LeaderState.Start(_stateMachine);

        bool QuorumReached()
        {
            return Node.PeerGroup.IsQuorumReached(votes);
        }
    }


    public async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < Node.CurrentTerm)
        {
            return new RequestVoteResponse()
            {
                CurrentTerm = Node.CurrentTerm,
                VoteGranted = false
            };
        }

        var canVote =
            Node.VotedFor is null || 
            Node.VotedFor == request.CandidateId;
        
        if (canVote && _stateMachine.Log.LastLogEntry.IsUpToDateWith(request.LastLog))
        {
            Node.CurrentTerm = request.CandidateTerm;
            Node.VotedFor = request.CandidateId;
            
            return new RequestVoteResponse()
            {
                CurrentTerm = Node.CurrentTerm,
                VoteGranted = true,
            };
        }
        
        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse()
        {
            CurrentTerm = Node.CurrentTerm, 
            VoteGranted = false
        };
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    private void OnElectionTimerTimeout()
    {
        _cts.Cancel();
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");
        _stateMachine.CurrentState = Start(_stateMachine);
    }

    public void Dispose()
    {
        _cts?.Dispose();
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
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