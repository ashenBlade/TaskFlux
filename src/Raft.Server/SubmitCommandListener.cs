using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;
using Raft.Core.Node;
using Serilog;

namespace Raft.Server;

public class SubmitCommandListener
{
    private static readonly Encoding Encoding = Encoding.UTF8;
    private readonly INode _node;
    private readonly int _port;
    private readonly ILogger _logger;

    public SubmitCommandListener(INode node, int port, ILogger logger)
    {
        _node = node;
        _port = port;
        _logger = logger;
    }
    
    public async Task RunAsync(CancellationToken token)
    {
        await Task.Yield();
        var tcs = new TaskCompletionSource();
        await using var _ = token.Register(() => tcs.SetCanceled(token));
        _logger.Information("Создаю HttpListener");
        var listener = new HttpListener()
        {
            Prefixes = {$"http://+:{_port}/"},
        };
        
        _logger.Information("Запускаю HttpListener");
        listener.Start();
        try
        {
            _logger.Information("Начинаю принимать запросы от клиентов");
            while (token.IsCancellationRequested is false)
            {
                var context = await listener.GetContextAsync();
                _logger.Debug("Получен новый запрос");
                context.Response.KeepAlive = false;
                context.Response.ContentType = "application/json";
                
                try
                {
                    if (!context.Request.HasEntityBody)
                    {
                        _logger.Debug("В теле запроса не было данных");
                        await RespondEmptyBodyNotAcceptedAsync(context);
                        continue;
                    }
                
                    var body = await GetRequestBodyAsync(context);
                    _logger.Debug("Принял запрос от клиента. Тело: {Body}", body);
                    var response = _node.Handle(new SubmitRequest(body));
                    _logger.Debug("Команда успешно выполнилась");
                    await RespondSuccessAsync(context, response);
                }
                catch (InvalidOperationException)
                {
                    _logger.Debug("Текущий узел не лидер");
                    await RespondNotLeaderAsync(context);
                }
                finally
                {
                    context.Response.Close();
                }
            }
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task RespondSuccessAsync(HttpListenerContext context,
                                           SubmitResponse response)
    {
        context.Response.StatusCode = ( int ) HttpStatusCode.Created;
        await using var writer = new StreamWriter(context.Response.OutputStream);
        await writer.WriteAsync(SerializeResponse(true, null, response.CreatedEntry));
    }

    private async Task RespondNotLeaderAsync(HttpListenerContext context)
    {
        context.Response.StatusCode = ( int ) HttpStatusCode.MisdirectedRequest;
        await using var writer = new StreamWriter(context.Response.OutputStream, Encoding);
        await writer.WriteAsync(SerializeResponse(false, "Текущий узел не лидер"));
    }
    

    private async Task RespondEmptyBodyNotAcceptedAsync(HttpListenerContext context)
    {
        await using var writer = new StreamWriter(context.Response.OutputStream, Encoding, leaveOpen: true);
        context.Response.StatusCode = (int)HttpStatusCode.UnprocessableEntity;
        await writer.WriteAsync(SerializeResponse(false, "В теле запроса не указаны данные"));
    }

    private async Task<string> GetRequestBodyAsync(HttpListenerContext context)
    {
        using var reader = new StreamReader(context.Request.InputStream, Encoding);
        return await reader.ReadToEndAsync();
    }

    private string SerializeResponse(bool success, string? message, LogEntry? createdEntry = null)
    {
        return JsonSerializer.Serialize(new Response()
        {
            Message = message,
            Success = success,
            CreatedEntry = createdEntry
        }, new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });
    }


    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
    private class Response
    {
        public bool Success { get; set; }
        public string? Message { get; set; } 
        public LogEntry? CreatedEntry { get; set; }
    }
}