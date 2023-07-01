using System.Net;
using System.Text;
using Raft.Core.Node;

namespace Raft.Host.HttpModule;

public class PrometheusRequestHandler: IRequestHandler
{
    private readonly INode _node;

    public PrometheusRequestHandler(INode node)
    {
        _node = node;
    }
    
    public async Task HandleRequestAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken token)
    {
        var builder = new StringBuilder();
        {
            builder.AppendLine("# TYPE raft_current_role gauge");
            builder.AppendLine("# HELP raft_current_role Current role of node");
            builder.AppendLine($@"raft_current_role {( int ) _node.CurrentRole} 1688183586760");
        }

        builder.AppendLine();
        
        {
            builder.AppendLine("# TYPE raft_current_term gauge");
            builder.AppendLine("# HELP raft_current_term Current term of node");
            builder.AppendLine($@"raft_current_term {_node.CurrentTerm.Value} 1688183586760");
        }

        builder.AppendLine();
        builder.AppendLine("# EOF");
        
        response.ContentType = "text/plain; charset=utf-8";
        response.StatusCode = 200;
        
        await using var writer = new StreamWriter(response.OutputStream, Encoding.UTF8);
        await writer.WriteAsync(builder.ToString());
    }
}