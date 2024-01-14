using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Hosting;
using Serilog;
using TaskFlux.Transport.Common;

namespace TaskFlux.Transport.Tcp;

public class SocketRequestModule : BackgroundService
{
    private readonly IRequestAcceptor _requestAcceptor;
    private readonly SocketRequestModuleOptions _options;
    private readonly IApplicationInfo _applicationInfo;
    private readonly ILogger _logger;

    public SocketRequestModule(IRequestAcceptor requestAcceptor,
                               SocketRequestModuleOptions options,
                               IApplicationInfo applicationInfo,
                               ILogger logger)
    {
        _requestAcceptor = requestAcceptor;
        _options = options;
        _applicationInfo = applicationInfo;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken token)
    {
        _logger.Information("Модуль бинарных запросов инициализируется");
        var port = _options.Port;
        var listener = new TcpListener(IPAddress.Any, port);
        var backlogSize = _options.BacklogSize;
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
                    Log.ForContext<ClientRequestProcessor>());

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

    protected override async Task ExecuteAsync(CancellationToken token)
    {
        _logger.Information("Модуль TCP запросов запускается");
        var port = _options.Port;
        var listener = new TcpListener(IPAddress.Any, port);
        var backlogSize = _options.BacklogSize;
        _logger.Debug("Инициализирую сокет сервера на порту {Port} с размером бэклога {BacklogSize}", port,
            backlogSize);
        listener.Start(backlogSize);
        _logger.Information("Инициализация закончилась. Начинаю принимать входящие запросы");
        try
        {
            while (token.IsCancellationRequested is false)
            {
                var client = await listener.AcceptTcpClientAsync(token);
                var processor = new ClientRequestProcessor(client, _requestAcceptor, _options, _applicationInfo,
                    Log.ForContext<ClientRequestProcessor>());
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