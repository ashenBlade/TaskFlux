using System.Net;
using Serilog;

namespace Raft.Host.HttpModule;

public class HttpRequestModule
{
    private readonly int _port;
    private readonly ILogger _logger;
    private readonly Dictionary<string, Dictionary<string, IRequestHandler>> _handlers = new();

    public HttpRequestModule(int port, ILogger logger)
    {
        _port = port;
        _logger = logger;
    }

    public void AddHandler(HttpMethod method, string path, IRequestHandler handler)
    {
        var pathHandler = _handlers.TryGetValue(method.Method, out var map)
                              ? map
                              : _handlers[method.Method] = new Dictionary<string, IRequestHandler>();
        pathHandler[path] = handler;
    }

    private HttpListener CreateHttpListener()
    {
        var prefix = $"http://+:{_port}/";
        _logger.Debug("Прослушиваемый адрес: {Prefix}", prefix);
        var listener = new HttpListener()
        {
            Prefixes =
            {
                prefix
            }
        };
        return listener;
    }
    
    public async Task RunAsync(CancellationToken token)
    {
        await Task.Yield();
        using var listener = CreateHttpListener();
        _logger.Information("Запускаю прослушивание HTTP запросов");
        listener.Start();
        _logger.Information("Начинаю принимать запросы");
        try
        {
            while (token.IsCancellationRequested is false)
            {
                var context = await listener.GetContextAsync();
                _ = ProcessRequestAsync(context, token);
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task ProcessRequestAsync(HttpListenerContext context, CancellationToken token)
    {
        await Task.Yield();
        _logger.Debug("Получен запрос {Url}", context.Request.Url);
        var response = context.Response;
        var request = context.Request;
        
        try
        {
            if (TryGetHandler(request.HttpMethod.ToUpper(), request.Url!.AbsolutePath, out var handler))
            {
                _logger.Debug("По запрошенному пути найден обработчик");
                try
                {
                    await handler.HandleRequestAsync(request, response, token);
                }
                catch (Exception e)
                {
                    _logger.Warning(e, "Во время работы обработчика поймано необработанное исключение");
                }
            }
            else
            {
                _logger.Debug("По запрошенному клиентом пути не нашлось обработчика");
                response.StatusCode = 404;
            }
        }
        finally
        {
            response.Close();
        }
    }

    private bool TryGetHandler(string method, string path, out IRequestHandler handler)
    {
        if (_handlers.TryGetValue(method, out var pathHandler) &&
            pathHandler.TryGetValue(path, out handler!))
        {
            return true;
        }
        handler = null!;
        return false;
    }
}