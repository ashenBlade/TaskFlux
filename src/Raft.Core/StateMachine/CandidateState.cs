using Raft.Core.Commands;
using Raft.Core.Peer;
using Serilog;

namespace Raft.Core.StateMachine;

public class CandidateState: INodeState
{
    private readonly RaftStateMachine _stateMachine;
    private readonly ILogger _logger;

    public CandidateState(RaftStateMachine stateMachine, ILogger logger)
    {
        _stateMachine = stateMachine;
        _logger = logger;
    }

    public async Task<RequestVoteResponse?[]> RunQuorumRound(List<IPeer> peers, CancellationToken token)
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
        peers.AddRange(Node.Peers);
        // Держим в уме узлы, которые не ответили взаимностью
        var swap = new List<IPeer>();
        var votes = 0;
        
        while (Node.Peers.Length / 2 > votes)
        {
            _logger.Debug("Начинаю раунд кворума. Отправляю запросы на {Count} узлов: {Peers}", peers.Count, peers.Select(x => x.Id));
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
                    swap.Add(Node.Peers[i]);
                    _logger.Verbose("Узел {NodeId} не вернул ответ", peers[i].Id);
                }
                // Узел согласился
                else if (response.VoteGranted)
                {
                    votes++;
                    _logger.Verbose("Узел {NodeId} отдал голос за", peers[i].Id);
                }
                // Узел отказался - у него более высокий Term
                else if (Node.CurrentTerm < response.CurrentTerm)
                {
                    _logger.Verbose("Узел {NodeId} отказался отдавать голос. Его терм {PeerTerm} больше моего {MyTerm}. Перехожу в Follower", peers[i].Id, response.CurrentTerm, Node.CurrentTerm);
                    _stateMachine.GoToFollowerState(response.CurrentTerm);
                    return;
                }
                else
                {
                    _logger.Verbose("Узел {NodeId} отказался отдавать голос, но его Term не больше моего {MyTerm}", peers[i].Id, Node.CurrentTerm);
                }
            }

            ( peers, swap ) = ( swap, peers );
            swap.Clear();
        }
        
        _logger.Debug("Кворум собран. Получено {VotesCount} голосов. Перехожду в состояние Leader", votes);
        _stateMachine.GoToLeaderState();
    }

    private IPeer[] Peers =>
        Node.Peers;

    private Node Node =>
        _stateMachine.Node;

    public async Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        throw new NotImplementedException();
    }
}