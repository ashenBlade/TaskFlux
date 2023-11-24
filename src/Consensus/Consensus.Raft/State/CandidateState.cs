using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.InstallSnapshot;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Commands.Submit;
using Serilog;
using TaskFlux.Models;

namespace Consensus.Raft.State;

public class CandidateState<TCommand, TResponse>
    : State<TCommand, TResponse>
{
    public override NodeRole Role => NodeRole.Candidate;
    private readonly ITimer _electionTimer;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;

    internal CandidateState(IRaftConsensusModule<TCommand, TResponse> raftConsensusModule,
                            ITimer electionTimer,
                            ILogger logger)
        : base(raftConsensusModule)
    {
        _electionTimer = electionTimer;
        _logger = logger;
        _cts = new();
    }

    public override void Initialize()
    {
        BackgroundJobQueue.RunInfinite(RunQuorum, _cts.Token);

        _electionTimer.Timeout += OnElectionTimerTimeout;
        _electionTimer.Schedule();
    }

    private async Task<RequestVoteResponse?[]> SendRequestVotes(List<IPeer> peers, CancellationToken token)
    {
        // Отправляем запрос всем пирам.
        // Стандартный Scatter/Gather паттерн
        var request = new RequestVoteRequest(CandidateId: Id,
            CandidateTerm: CurrentTerm,
            LastLogEntryInfo: PersistenceFacade.LastEntry);

        var requests = new Task<RequestVoteResponse?>[peers.Count];
        for (var i = 0; i < peers.Count; i++)
        {
            requests[i] = peers[i].SendRequestVoteAsync(request, token);
        }

        return await Task.WhenAll(requests);
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
    private async Task RunQuorum()
    {
        CancellationToken token;
        try
        {
            token = _cts.Token;
        }
        catch (ObjectDisposedException)
        {
            _logger.Debug("Не удалось запустить кворум: источник токенов утилизирован");
            return;
        }

        try
        {
            await RunQuorumInner(token);
        }
        catch (OperationCanceledException) when
            (_cts.IsCancellationRequested)
        {
            _logger.Debug("Сбор кворума прерван - задача отменена");
        }
        catch (ObjectDisposedException ode) when (ode.ObjectName == nameof(CancellationTokenSource))
        {
            _logger.Debug("Сбор кворума прерван - задача отменена");
        }
        catch (Exception unhandled)
        {
            _logger.Fatal(unhandled, "Поймано необработанное исключение во время запуска кворума");
        }
    }

    private async Task RunQuorumInner(CancellationToken token)
    {
        _logger.Debug("Запускаю кворум для получения большинства голосов");
        var term = CurrentTerm;

        // Список из узлов, на которые нужно отправить запросы.
        // В начале инициализируется всеми узлами кластера, 
        // когда ответил (хоть как-то) будет удален из списка
        var leftPeers = new List<IPeer>(PeerGroup.Peers.Count);
        leftPeers.AddRange(PeerGroup.Peers);

        // Вспомогательный список из узлов, которые не ответили.
        // Используется для обновления leftPeers
        var notResponded = new List<IPeer>();

        // Количество узлов, которые отдали за нас голос
        var votes = 0;
        _logger.Debug("Начинаю раунд кворума для терма {Term}. Отправляю запросы на узлы: {Peers}", term,
            leftPeers.Select(x => x.Id));
        while (!QuorumReached() && !token.IsCancellationRequested)
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
                    _logger.Verbose("Узел {NodeId} имеет более высокий Term. Перехожу в состояние Follower",
                        leftPeers[i].Id);
                    _cts.Cancel();

                    var followerState = RaftConsensusModule.CreateFollowerState();
                    if (RaftConsensusModule.TryUpdateState(followerState, this))
                    {
                        RaftConsensusModule.PersistenceFacade.UpdateState(response.CurrentTerm, null);
                    }

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
                _logger.Debug(
                    "Кворум не достигнут и нет узлов, которым можно послать запросы. Дожидаюсь завершения таймаута выбора для перехода в следующий терм");
                return;
            }
        }

        _logger.Debug("Кворум собран. Получено {VotesCount} голосов. Посылаю команду перехода в состояние Leader",
            votes);

        if (token.IsCancellationRequested)
        {
            _logger.Debug("Токен был отменен. Кворум не достигнут");
            return;
        }

        var leaderState = RaftConsensusModule.CreateLeaderState();
        RaftConsensusModule.TryUpdateState(leaderState, this);

        bool QuorumReached()
        {
            return PeerGroup.IsQuorumReached(votes);
        }
    }

    private void OnElectionTimerTimeout()
    {
        _electionTimer.Timeout -= OnElectionTimerTimeout;

        var candidateState = RaftConsensusModule.CreateCandidateState();
        if (RaftConsensusModule.TryUpdateState(candidateState, this))
        {
            _logger.Debug("Сработал Election Timeout. Перехожу в новый терм");
            RaftConsensusModule.PersistenceFacade.UpdateState(RaftConsensusModule.CurrentTerm.Increment(),
                RaftConsensusModule.Id);
        }
    }

    public override AppendEntriesResponse Apply(AppendEntriesRequest request)
    {
        if (request.Term < CurrentTerm)
        {
            return AppendEntriesResponse.Fail(CurrentTerm);
        }

        var followerState = RaftConsensusModule.CreateFollowerState();
        RaftConsensusModule.TryUpdateState(followerState, this);
        return RaftConsensusModule.Handle(request);
    }

    public override SubmitResponse<TResponse> Apply(SubmitRequest<TCommand> request, CancellationToken token = default)
    {
        return SubmitResponse<TResponse>.NotLeader;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        // Мы в более актуальном Term'е
        if (request.CandidateTerm < CurrentTerm)
        {
            return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
        }

        var logConflicts = PersistenceFacade.Conflicts(request.LastLogEntryInfo);
        if (logConflicts)
        {
            _logger.Debug("При обработке RequestVote от узла {NodeId} обнаружен конфликт лога", request.CandidateId);
        }

        if (CurrentTerm < request.CandidateTerm)
        {
            var followerState = RaftConsensusModule.CreateFollowerState();
            if (RaftConsensusModule.TryUpdateState(followerState, this))
            {
                RaftConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, null);
            }

            return new RequestVoteResponse(CurrentTerm, !logConflicts);
        }

        var canVote =
            // Ранее не голосовали
            VotedFor is null
          ||
            // Текущий лидер/кандидат посылает этот запрос (почему бы не согласиться)
            VotedFor == request.CandidateId;

        // Отдать свободный голос можем только за кандидата 
        if (canVote
          &&
            // У которого лог в консистентном с нашим состоянием
            !logConflicts)
        {
            var followerState = RaftConsensusModule.CreateFollowerState();
            if (RaftConsensusModule.TryUpdateState(followerState, this))
            {
                RaftConsensusModule.PersistenceFacade.UpdateState(request.CandidateTerm, null);
                return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: true);
            }

            return RaftConsensusModule.Handle(request);
        }

        // Кандидат только что проснулся и не знает о текущем состоянии дел. 
        // Обновим его
        return new RequestVoteResponse(CurrentTerm: CurrentTerm, VoteGranted: false);
    }

    public override void Dispose()
    {
        _electionTimer.Stop();
        _cts.Cancel();
        _electionTimer.Timeout -= OnElectionTimerTimeout;

        _cts.Dispose();
        _electionTimer.Dispose();
    }

    public override IEnumerable<InstallSnapshotResponse> Apply(InstallSnapshotRequest request,
                                                               CancellationToken token = default)
    {
        if (request.Term < CurrentTerm)
        {
            return new[] {new InstallSnapshotResponse(CurrentTerm)};
        }

        var state = RaftConsensusModule.CreateFollowerState();
        if (RaftConsensusModule.TryUpdateState(state, this))
        {
            _logger.Debug("Получен InstallSnapshotRequest с термом не меньше моего. Перехожу в Follower");
        }
        else
        {
            _logger.Debug("Получен InstallSnapshotRequest с термом не меньше моего, но перейти в Follower не удалось");
        }

        return RaftConsensusModule.Handle(request, token);
    }
}