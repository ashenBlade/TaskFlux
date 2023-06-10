using Raft.Core.Commands;
using Serilog;

namespace Raft.Core.StateMachine;

public class RaftStateMachine: INodeState, IDisposable
{
    private readonly ILogger _logger;
    internal Node Node { get; }
    public INodeState CurrentState { get; internal set; } = null!;
    internal ITimer ElectionTimer { get; }
    internal ITimer HeartbeatTimer { get; }

    public RaftStateMachine(Node node, ILogger logger, ITimer electionTimer, ITimer heartbeatTimer)
    {
        Node = node;
        _logger = logger;
        ElectionTimer = electionTimer;
        HeartbeatTimer = heartbeatTimer;
        GoToFollowerState();
    }
    

    public Task<RequestVoteResponse> Apply(RequestVoteRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }

    public Task<HeartbeatResponse> Apply(HeartbeatRequest request, CancellationToken token)
    {
        return CurrentState.Apply(request, token);
    }

    internal void GoToFollowerState()
    {
        var state = new FollowerState(Node, this, _logger);
        ElectionTimer.Timeout += ElectionTimeoutCallback;
        CurrentState = state;
        ElectionTimer.Start();
    }

    internal void GoToFollowerState(Term term)
    {
        var state = new FollowerState(Node, this, _logger);
        ElectionTimer.Timeout += ElectionTimeoutCallback;
        Node.CurrentTerm = term;
        CurrentState = state;
        ElectionTimer.Start();
    }

    internal void GoToCandidateState()
    {
        var state = new CandidateState(this, _logger);
        CurrentState = state;
        
        Node.VotedFor = Node.Id;
        Node.CurrentTerm = Node.CurrentTerm.Increment();
        
        var cts = new CancellationTokenSource();
        
        _ = state.RunQuorum(cts.Token)
                 .ContinueWith(x => cts.Dispose(), cts.Token);

        void OnElectionTimerOnTimeout()
        {
            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                _logger.Verbose("ElectionTimeout для Candidate сработал, но CTS уже вызвал Dispose");
                return;
            }

            _logger.Debug("ElectionTimeout сработал во время сбора кворума. Перехожу в новый Term");
            cts.Dispose();
            ElectionTimer.Timeout -= OnElectionTimerOnTimeout;
            GoToCandidateState();
        }

        ElectionTimer.Timeout += OnElectionTimerOnTimeout;

        ElectionTimer.Reset();
    }
    
    public void GoToLeaderState()
    {
        CurrentState = new LeaderState(this, _logger);
        ElectionTimer.Stop();
        HeartbeatTimer.Start();
    }

    private void ElectionTimeoutCallback()
    {
        _logger.Debug("Вызван ElectionTimeout callback. Перехожу в состояние Candidate");
        ElectionTimer.Timeout -= ElectionTimeoutCallback;
        GoToCandidateState();
    }

    public void Dispose()
    {
        ElectionTimer.Timeout -= ElectionTimeoutCallback;
    }

}