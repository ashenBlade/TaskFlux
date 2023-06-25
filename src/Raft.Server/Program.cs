﻿using System.ComponentModel.DataAnnotations;
using System.Net;
using Microsoft.Extensions.Configuration;
using Raft.CommandQueue;
using Raft.Core;
using Raft.Core.Log;
using Raft.Core.Node;
using Raft.JobQueue;
using Raft.Log.InMemory;
using Raft.Network.Socket;
using Raft.Peer;
using Raft.Peer.Decorators;
using Raft.Server;
using Raft.Server.HttpModule;
using Raft.Server.Infrastructure;
using Raft.Server.Options;
using Raft.Timers;
using Serilog;
// ReSharper disable CoVariantArrayConversion

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.ffff} {Level:u3}] ({SourceContext}) {Message}{NewLine}{Exception}")
            .CreateLogger();

var configuration = new ConfigurationBuilder()
                   .AddEnvironmentVariables()
                   .Build();

var options = configuration.Get<RaftServerOptions>() 
           ?? throw new Exception("Не найдено настроек");

ValidateOptions(options);

var nodeId = new NodeId(options.NodeId);

var peers = options.Peers
                   .Select(p =>
                    {
                        var endpoint = GetEndpoint(p.Host, p.Port);
                        var id = new NodeId(p.Id);
                        var connection = new RemoteSocketNodeConnection(endpoint, Log.ForContext("SourceContext", $"RemoteSocketNodeConnection({id.Value})"));
                        IPeer peer = new TcpPeer(connection, id, nodeId, Log.ForContext("SourceContext", $"TcpPeer({id.Value})"));
                        peer = new ExclusiveAccessPeerDecorator(peer);
                        peer = new NetworkExceptionDelayPeerDecorator(peer, TimeSpan.FromMilliseconds(250));
                        return peer;
                    })
                   .ToArray();

Log.Logger.Information("Узлы кластера: {Peers}", options.Peers);

using var electionTimer = new RandomizedTimer(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5));
using var heartbeatTimer = new SystemTimersTimer( TimeSpan.FromSeconds(1) );

var log = new InMemoryLog(Enumerable.Empty<LogEntry>());
var jobQueue = new TaskJobQueue(Log.Logger.ForContext<TaskJobQueue>());

using var commandQueue = new ChannelCommandQueue();
using var node = RaftNode.Create(nodeId, new PeerGroup(peers), null, new Term(1), Log.ForContext<RaftNode>(), electionTimer, heartbeatTimer, jobQueue, log, commandQueue, new NullStateMachine());
var connectionManager = new ExternalConnectionManager(options.Host, options.Port, node, Log.Logger.ForContext<ExternalConnectionManager>());
var server = new RaftStateObserver(node, Log.Logger.ForContext<RaftStateObserver>());

var httpModule = CreateHttpRequestModule(configuration);

httpModule.AddHandler(HttpMethod.Post, "/command", new SubmitCommandRequestHandler(node, Log.ForContext<SubmitCommandRequestHandler>()));

using var cts = new CancellationTokenSource();

// ReSharper disable once AccessToDisposedClosure
Console.CancelKeyPress += (_, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

try
{
    Log.Logger.Information("Запускаю Election Timer");
    node.ElectionTimer.Start();
    Log.Logger.Information("Запукаю фоновые задачи");
    await Task.WhenAll(
        server.RunAsync(cts.Token), 
        connectionManager.RunAsync(cts.Token), 
        commandQueue.RunAsync(cts.Token),
        httpModule.RunAsync(cts.Token));
}
catch (Exception e)
{
    Log.Fatal(e, "Ошибка во время работы сервера");
}

void ValidateOptions(RaftServerOptions peersOptions)
{
    var errors = new List<ValidationResult>();
    if (!Validator.TryValidateObject(peersOptions, new ValidationContext(peersOptions), errors, true))
    {
        throw new Exception($"Найдены ошибки при валидации конфигурации: {string.Join(',', errors.Select(x => x.ErrorMessage))}");
    }
}

HttpRequestModule CreateHttpRequestModule(IConfiguration config)
{
    var httpModuleOptions = config.GetRequiredSection("HTTP")
                               .Get<HttpModuleOptions>()
               ?? throw new ApplicationException("Настройки для HTTP модуля не найдены");

    var module = new HttpRequestModule(httpModuleOptions.Port, Log.ForContext<HttpRequestModule>());

    return module;
}

EndPoint GetEndpoint(string host, int port)
{
    if (IPAddress.TryParse(host, out var ip))
    {
        return new IPEndPoint(ip, port);
    }

    return new DnsEndPoint(host, port);
}
