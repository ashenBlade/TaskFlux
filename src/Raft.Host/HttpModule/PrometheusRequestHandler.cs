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
            builder.AppendLine($@"raft_current_role {( int ) _node.CurrentRole} 4");
        }

        builder.AppendLine();
        
        {
            builder.AppendLine("# TYPE raft_current_term gauge");
            builder.AppendLine("# HELP raft_current_term Current term of node");
            builder.AppendLine($@"raft_current_term {_node.CurrentTerm.Value} 4");
        }

        builder.AppendLine("\n# EOF");
        
        response.ContentType = "text/plain; charset=utf-8; version=0.0.4";
        response.Headers.Add("Last-Modified", ( DateTime.Now - TimeSpan.FromSeconds(1) ).ToString("R"));
        response.StatusCode = (int)HttpStatusCode.OK;
        response.KeepAlive = false;
        response.ContentEncoding = Encoding.UTF8;
        response.SendChunked = true;
        
        var bytes = Encoding.UTF8.GetBytes(builder.ToString());
        response.ContentLength64 = bytes.Length;
        await response.OutputStream.WriteAsync(bytes, token);
    }
}