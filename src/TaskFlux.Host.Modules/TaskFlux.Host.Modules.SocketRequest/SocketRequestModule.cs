using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using Serilog;
using TaskFlux.Core;

namespace TaskFlux.Host.Modules.SocketRequest;

public class SocketRequestModule
{
    private readonly IRequestAcceptor _requestAcceptor;
    private readonly IOptionsMonitor<SocketRequestModuleOptions> _options;
    private readonly IClusterInfo _clusterInfo;
    private readonly IApplicationInfo _applicationInfo;
    private readonly ILogger _logger;

    public SocketRequestModule(IRequestAcceptor requestAcceptor,
                               IOptionsMonitor<SocketRequestModuleOptions> options,
                               IClusterInfo clusterInfo,
                               IApplicationInfo applicationInfo,
                               ILogger logger)
    {
        _requestAcceptor = requestAcceptor;
        _options = options;
        _clusterInfo = clusterInfo;
        _applicationInfo = applicationInfo;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken token)
    {
        _logger.Information("Модуль бинарных запросов инициализируется");
        var port = _options.CurrentValue.Port;
        var listener = new TcpListener(IPAddress.Any, port);
        var backlogSize = _options.CurrentValue.BacklogSize;
        _logger.Debug("Инициализирую сокет сервера на порту {Port} с размером бэклога {BacklogSize}", port,
            backlogSize);
        listener.Start(backlogSize);
        _logger.Information("Инициализация модуля закончилась. Начинаю принимать входящие запросы");
        try
        {
            while (token.IsCancellationRequested is false)
            {
                var client = await listener.AcceptTcpClientAsync(token);
                var processor = new ClientRequestProcessor(client, _requestAcceptor, _options, _applicationInfo,
                    _clusterInfo, Log.ForContext<ClientRequestProcessor>());
                // MAYBE: может лучше свой пул потоков для обработки клиентов?
                // TODO: закрытие обработчика запроса при закрытии приложения
                _ = processor.ProcessAsync(token);
            }
        }
        catch (OperationCanceledException)
            when (token.IsCancellationRequested)
        {
        }
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