using Raft.Core;
using Serilog;

namespace Raft.Server;

public class RaftServer
{
    private readonly ILogger _logger;

    public RaftServer(ILogger logger)
    {
        _logger = logger;
    }

    public async Task RunAsync()
    {
        // Создаем ноду в изначальном состоянии
        var node = new Node();
        
        //  
        throw new NotImplementedException();
    }
}