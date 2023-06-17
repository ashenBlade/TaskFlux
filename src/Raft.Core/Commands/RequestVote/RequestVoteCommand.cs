using Raft.Core.Node;

namespace Raft.Core.Commands.RequestVote;

internal class RequestVoteCommand: Command<RequestVoteResponse>
{
    private readonly RequestVoteRequest _request;

    public RequestVoteCommand(RequestVoteRequest request, INode node) : base(node)
    {
        _request = request;
    }

    public override RequestVoteResponse Execute()
    {
        return Node.CurrentState.Apply(_request);
    }
}