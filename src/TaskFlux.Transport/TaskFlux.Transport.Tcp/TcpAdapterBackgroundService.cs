using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Hosting;
using Serilog;
using TaskFlux.Application;
using IApplicationLifetime = TaskFlux.Application.IApplicationLifetime;

namespace TaskFlux.Transport.Tcp;

public class TcpAdapterBackgroundService : BackgroundService
{
    private readonly IRequestAcceptor _requestAcceptor;
    private readonly TcpAdapterOptions _options;
    private readonly IApplicationInfo _applicationInfo;
    private readonly IApplicationLifetime _lifetime;
    private readonly ILogger _logger;

    public TcpAdapterBackgroundService(IRequestAcceptor requestAcceptor,
        TcpAdapterOptions options,
        IApplicationInfo applicationInfo,
        IApplicationLifetime lifetime,
        ILogger logger)
    {
        _requestAcceptor = requestAcceptor;
        _options = options;
        _applicationInfo = applicationInfo;
        _lifetime = lifetime;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken token)
    {
        await Task.Yield();

        _logger.Information("Модуль TCP запросов запускается");
        var port = _options.TcpAdapterListenPort;
        var listener = new TcpListener(IPAddress.Any, port);
        var backlogSize = _options.TcpAdapterBacklogSize;
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
                    _lifetime,
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
            _lifetime.StopAbnormal();
        }
        finally
        {
            _logger.Information("Модуль запросов заканчивает работу");
            listener.Stop();
        }
    }
}