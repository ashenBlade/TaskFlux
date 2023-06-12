// See https://aka.ms/new-console-template for more information

using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
using Raft.JobQueue;
using Raft.Log;
using Raft.Peer;
using Raft.Server;
using Raft.Timers;
using Serilog;

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:dd.ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();
var responseDelay = TimeSpan.FromSeconds(1);
var peers = Enumerable.Range(2, 2)
                      .Select(i => ( IPeer ) new LambdaPeer(i, async r =>
                       {
                           await Task.Delay(responseDelay);
                           return HeartbeatResponse.Ok(r.Term);
                       }, async r =>
                       {
                           await Task.Delay(responseDelay);
                           return new RequestVoteResponse() {VoteGranted = true, CurrentTerm = new Term(1)};
                       }))
                      .ToArray();

using var electionTimer = new SystemTimersTimer( TimeSpan.FromSeconds(5) );
using var heartbeatTimer = new SystemTimersTimer( TimeSpan.FromSeconds(1) );

var log = new InMemoryLog();
var jobQueue = new TaskJobQueue(Log.Logger.ForContext<TaskJobQueue>());

var server = new RaftServer(Log.Logger.ForContext<RaftServer>(), peers, log, jobQueue, electionTimer, heartbeatTimer);
using var cts = new CancellationTokenSource();

// ReSharper disable once AccessToDisposedClosure
Console.CancelKeyPress += (_, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

await server.RunAsync(cts.Token);