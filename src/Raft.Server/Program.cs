// See https://aka.ms/new-console-template for more information

using Raft.Server;
using Serilog;

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.Console()
            .CreateLogger();

var server = new RaftServer(Log.Logger);
using var cts = new CancellationTokenSource();

// ReSharper disable once AccessToDisposedClosure
Console.CancelKeyPress += (_, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

await server.RunAsync(cts.Token);