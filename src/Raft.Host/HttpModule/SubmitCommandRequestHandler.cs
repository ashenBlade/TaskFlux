using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raft.Core.Commands.Submit;
using Raft.Core.Log;
using Raft.Core.Node;
using Serilog;

namespace Raft.Host.HttpModule;

public class SubmitCommandRequestHandler: IRequestHandler
{
    private static readonly Encoding Encoding = Encoding.UTF8;
    private readonly INode _node;
    private readonly ILogger _logger;

    public SubmitCommandRequestHandler(INode node, ILogger logger)
    {
        _node = node;
        _logger = logger;
    }
    
    public async Task HandleRequestAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken token)
    {
        response.KeepAlive = false;
        response.ContentType = "application/json";
        
        if (!request.HasEntityBody)
        {
            _logger.Debug("В теле запроса не было данных");
            await RespondEmptyBodyNotAcceptedAsync(response);
            return;
        }
        
        var body = await GetRequestBodyAsync(request);
        _logger.Debug("Принял запрос от клиента. Тело: {Body}", body);

        var submitResponse = _node.Handle(new SubmitRequest(body));

        if (submitResponse.WasLeader)
        {
            RespondSuccessAsync(response, submitResponse);
            return;
        }
        
        await RespondNotLeaderAsync(response);
    }
    
    private void RespondSuccessAsync(HttpListenerResponse response,
                                           SubmitResponse submitResponse)
    {
        response.StatusCode = ( int ) HttpStatusCode.OK;
        submitResponse.Response.WriteTo(response.OutputStream);
    }

    private async Task RespondNotLeaderAsync(HttpListenerResponse response)
    {
        response.StatusCode = ( int ) HttpStatusCode.MisdirectedRequest;
        await using var writer = new StreamWriter(response.OutputStream, Encoding);
        await writer.WriteAsync(SerializeResponse(false, "Текущий узел не лидер"));
    }
    

    private async Task RespondEmptyBodyNotAcceptedAsync(HttpListenerResponse response)
    {
        await using var writer = new StreamWriter(response.OutputStream, Encoding, leaveOpen: true);
        response.StatusCode = (int)HttpStatusCode.UnprocessableEntity;
        await writer.WriteAsync(SerializeResponse(false, "В теле запроса не указаны данные"));
    }

    private async Task<byte[]> GetRequestBodyAsync(HttpListenerRequest request)
    {
        var memory = new MemoryStream((int)request.ContentLength64);
        await request.InputStream.CopyToAsync(memory);

        return memory.ToArray();
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