using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using Consensus.Core;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Core;

namespace TaskFlux.Host.Modules.BinaryRequest;

public class BinaryRequestModule
{
    private readonly IConsensusModule<Command, Result> _consensusModule;
    private readonly IOptionsMonitor<BinaryRequestModuleOptions> _options;
    private readonly IClusterInfo _clusterInfo;
    private readonly ILogger _logger;

    public BinaryRequestModule(IConsensusModule<Command, Result> consensusModule,
                               IOptionsMonitor<BinaryRequestModuleOptions> options,
                               IClusterInfo clusterInfo,
                               ILogger logger)
    {
        _consensusModule = consensusModule;
        _options = options;
        _clusterInfo = clusterInfo;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken token)
    {
        _logger.Information("Модуль бинарных запросов инициализируется");
        var port = _options.CurrentValue.Port;
        var listener = new TcpListener(IPAddress.Any, port);
        var backlogSize = _options.CurrentValue.BacklogSize;
        _logger.Debug("Инициализирую сокет сервера на порту {Port} с размером бэклога {BacklogSize}", port, backlogSize);
        listener.Start(backlogSize);
        _logger.Information("Инициализация сервера окончилась. Начинаю принимать входящие запросы");
        try
        {
            while (token.IsCancellationRequested is false)
            {
                var client = await listener.AcceptTcpClientAsync(token);
                var processor = new RequestProcessor(client, _consensusModule, _clusterInfo, Log.ForContext<RequestProcessor>());
                _ = processor.ProcessAsync(token);
            }
        }
        catch (OperationCanceledException) 
            when (token.IsCancellationRequested) 
        { }
        catch (Exception e)
        {
            _logger.Error(e, "Необработанное исключение во время работы модуля запросов");
        }
        finally
        {
            _logger.Information("Модуль запросов заканчивает работу");
            listener.Stop();
        }
    }
}