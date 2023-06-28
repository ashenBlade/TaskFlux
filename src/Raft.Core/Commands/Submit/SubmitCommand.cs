using Raft.Core.Node;

namespace Raft.Core.Commands.Submit;

public class SubmitCommand: Command<SubmitResponse>
{
    private readonly SubmitRequest _request;

    public SubmitCommand(SubmitRequest request, INode node) : base(node)
    {
        _request = request;
    }

    public override SubmitResponse Execute()
    {
        return Node.CurrentState.Apply(_request);
    }
}