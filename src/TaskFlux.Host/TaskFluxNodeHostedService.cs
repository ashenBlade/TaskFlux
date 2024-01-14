using Consensus.JobQueue;
using Consensus.NodeProcessor;
using Consensus.Raft;
using Microsoft.Extensions.Hosting;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Host.RequestAcceptor;

namespace TaskFlux.Host;

public class TaskFluxNodeHostedService : IHostedService
{
    private readonly ILogger _logger;
    private readonly RaftConsensusModule<Command, Response> _module;
    private readonly NodeConnectionManager _connectionManager;
    private readonly ExclusiveRequestAcceptor _requestAcceptor;
    private readonly ThreadPerWorkerBackgroundJobQueue _backgroundJobQueue;

    public TaskFluxNodeHostedService(RaftConsensusModule<Command, Response> module,
                                     ExclusiveRequestAcceptor requestAcceptor,
                                     ThreadPerWorkerBackgroundJobQueue backgroundJobQueue,
                                     NodeConnectionManager connectionManager,
                                     ILogger logger)
    {
        _logger = logger;
        _module = module;
        _requestAcceptor = requestAcceptor;
        _backgroundJobQueue = backgroundJobQueue;
        _connectionManager = connectionManager;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.Information("Запускаю потоки обработчики подключений к узлам кластера");
        _backgroundJobQueue.Start();

        _logger.Information("Запускаю таймер выборов");
        _module.Start();

        _logger.Information("Запускаю обработчик подключений узлов кластера");
        _connectionManager.Start();

        _logger.Information("Запускаю обработчик пользовательских запросов");
        _requestAcceptor.Start();

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.Information("Останавливаю обработчик узлов кластера");
        _connectionManager.Stop();
        _logger.Information("Останавливаю обработчик пользовательских запросов");
        _requestAcceptor.Stop();
        _logger.Information("Останавливаю потоки обработчики подключений к узлам кластера");
        _backgroundJobQueue.Stop();
        _module.Dispose();

        return Task.CompletedTask;
    }
}