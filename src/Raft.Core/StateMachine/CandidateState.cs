using Raft.Core.Commands;
using Raft.Core.Peer;
using Serilog;

namespace Raft.Core.StateMachine;

public class CandidateState: INodeState
{
    private readonly RaftStateMachine _stateMachine;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private IPeer[] Peers => Node.Peers;
    private Node Node => _stateMachine.Node;

    private CandidateState(RaftStateMachine stateMachine, ILogger logger)
    {
        _stateMachine = stateMachine;
        _logger = logger;
        _cts = new();
        _stateMachine.ElectionTimer.Timeout += OnElectionTimerTimeout;
        _ = RunQuorum(_cts.Token);
    }

    private async Task<RequestVoteResponse?[]> RunQuorumRound(List<IPeer> peers, CancellationToken token)
    {
        // Отправляем запрос всем пирам
        var request = new RequestVoteRequest()
        {
            CandidateId = _stateMachine.Node.Id,
            CandidateTerm = _stateMachine.Node.CurrentTerm,
            LastLog = _stateMachine.Node.LastLogEntry
        };

        var requests = new Task<RequestVoteResponse?>[peers.Count];
        for (var i = 0; i < peers.Count; i++)
        {
            requests[i] = peers[i].SendRequestVote(request, token);
        }

        return await Task.WhenAll( requests );
    }

    /// <summary>
    /// Запустить раунды кворума и попытаться получить большинство голосов
    /// </summary>
    /// <param name="token">Токен отмены</param>
    public async Task RunQuorum(CancellationToken token)
    {
        _logger.Debug("Запускаю кворум для получения большинства голосов");
        var peers = new List<IPeer>(Peers.Length);
        var term = Node.CurrentTerm;
        peers.AddRange(Node.Peers);
        // Держим в уме узлы, которые не ответили взаимностью
        var swap = new List<IPeer>();
        var votes = 0;
        
        while (!QuorumReached())
        {
            _logger.Debug("Начинаю раунд кворума для терма {Term}. Отправляю запросы на {Count} узлов: {Peers}", term, peers.Count, peers.Select(x => x.Id));
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

                _logger.Debug("Кворум не достигнут и нет узлов, которым можно послать запросы");
                break;
            }
        }
        
        _logger.Debug("Кворум собран. Получено {VotesCount} голосов. Перехожду в состояние Leader", votes);
        
        _stateMachine.CurrentState = LeaderState.Start(_stateMachine);

        bool QuorumReached()
        {
            return Node.Peers.Length / 2 <= votes;
        }
    }


    public async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    private void OnElectionTimerTimeout()
    {
        _cts.Cancel();
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        _stateMachine.CurrentState = Start(_stateMachine);
    }

    public void Dispose()
    {
        _cts?.Dispose();
        _stateMachine.ElectionTimer.Timeout -= OnElectionTimerTimeout;
    }

    internal static CandidateState Start(RaftStateMachine stateMachine)
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