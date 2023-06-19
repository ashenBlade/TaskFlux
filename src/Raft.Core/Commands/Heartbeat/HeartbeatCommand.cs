using Raft.Core.Node;

namespace Raft.Core.Commands.Heartbeat;

internal class HeartbeatCommand: Command<HeartbeatResponse>
{
    private readonly HeartbeatRequest _request;

    public HeartbeatCommand(HeartbeatRequest request, INode node)
        : base(node)
    {
        _request = request;
    }

    public override HeartbeatResponse Execute()
    {
        return Node.CurrentState.Apply(_request);
    }
}