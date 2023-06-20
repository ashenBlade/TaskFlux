using Raft.Core.Node;

namespace Raft.Core.Commands.AppendEntries;

public class AppendEntriesCommand : Command<AppendEntriesResponse>
{
    private readonly AppendEntriesRequest _request;

    public AppendEntriesCommand(AppendEntriesRequest request, INode node) : base(node)
    {
        _request = request;
    }

    public override AppendEntriesResponse Execute()
    {
        return Node.Handle(_request);
    }
}
