using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Serilog;

namespace Raft.Core.Node;

internal class  FollowerState: BaseNodeState
{
    public override NodeRole Role => NodeRole.Follower;
    private readonly ILogger _logger;

    internal FollowerState(INode node, ILogger logger)
        : base(node)
    {
        _logger = logger;
        Node.ElectionTimer.Timeout += OnElectionTimerTimeout;
    }

    public override RequestVoteResponse Apply(RequestVoteRequest request)
    {
        _logger.Verbose("Получен RequestVote");
        Node.CommandQueue.Enqueue(new ResetElectionTimerCommand(this, Node));
        return base.Apply(request);
    }

    public override HeartbeatResponse Apply(HeartbeatRequest request)
    {
        _logger.Verbose("Получен Heartbeat");
        Node.CommandQueue.Enqueue(new ResetElectionTimerCommand(this, Node));
        return base.Apply(request);
    }

    internal static FollowerState Create(INode node)
    {
        return new FollowerState(node, node.Logger.ForContext("SourceContext", "Follower"));
    }

    private void OnElectionTimerTimeout()
    {
        _logger.Debug("Сработал Election Timeout. Перехожу в состояние Candidate");
        Node.CommandQueue.Enqueue(new MoveToCandidateAfterElectionTimerTimeoutCommand(this, Node));
    }
    
    public override void Dispose()
    {
        Node.ElectionTimer.Timeout -= OnElectionTimerTimeout;
        base.Dispose();
    }
}