// See https://aka.ms/new-console-template for more information

using Raft.Server;
using Serilog;

Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .CreateLogger();
            
var server = new RaftServer(Log.Logger);

await server.RunAsync();