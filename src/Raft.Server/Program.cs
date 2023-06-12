// See https://aka.ms/new-console-template for more information

using System.Text;
using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Peer;
using Raft.JobQueue;
using Raft.Log;
using Raft.Peer;
using Raft.Peer.InMemory;
using Raft.Server;
using Raft.Timers;
using Serilog;
// var responseDelay = TimeSpan.FromSeconds(1);
// var peers = Enumerable.Range(2, 2)
//                       .Select(i => ( IPeer ) new LambdaPeer(i, async r =>
//                        {
//                            await Task.Delay(responseDelay);
//                            return HeartbeatResponse.Ok(r.Term);
//                        }, async r =>
//                        {
//                            await Task.Delay(responseDelay);
//                            return new RequestVoteResponse() {VoteGranted = true, CurrentTerm = new Term(1)};
//                        }))
//                       .ToArray();


Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:dd.ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();

var peerPort = GetEnvInt("PEER_PORT");
var peerHost = GetEnv("PEER_HOST");
var peerId = new PeerId(GetEnvInt("PEER_ID"));
var listenPort = GetEnvInt("LISTEN_PORT");
var listenHost = GetEnv("LISTEN_HOST");
var nodeId = new PeerId(GetEnvInt("NODE_ID"));

var peers = new IPeer[]
{
    TcpPeer.Create(peerId, nodeId, peerHost, peerPort),
};

using var electionTimer = new RandomizedSystemTimersTimer(TimeSpan.FromSeconds(-1), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5) );
using var heartbeatTimer = new SystemTimersTimer( TimeSpan.FromSeconds(1) );

var log = new InMemoryLog();
var jobQueue = new TaskJobQueue(Log.Logger.ForContext<TaskJobQueue>());
var connectionManager = new ExternalConnectionsManager(listenHost, listenPort, Log.Logger.ForContext<ExternalConnectionsManager>());

var server = new RaftServer(nodeId, Log.Logger.ForContext<RaftServer>(), connectionManager, peers, log, jobQueue, electionTimer, heartbeatTimer);
using var cts = new CancellationTokenSource();

// ReSharper disable once AccessToDisposedClosure
Console.CancelKeyPress += (_, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

try
{
    await server.RunAsync(cts.Token);
}
catch (Exception e)
{
    Log.Fatal(e, "Ошибка во время работы сервера");
}

string GetEnv(string name) => Environment.GetEnvironmentVariable(name) 
                           ?? throw new ArgumentException($"{name} не указан");

int GetEnvInt(string name) => int.Parse(GetEnv(name));