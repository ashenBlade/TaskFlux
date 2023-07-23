using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Consensus.Core;
using Consensus.Core.Commands.Submit;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Core;

namespace TaskFlux.Host.Modules.HttpRequest;

public class SubmitCommandRequestHandler: IRequestHandler
{
    public IClusterInfo ClusterInfo { get; }
    private static readonly Encoding Encoding = Encoding.UTF8;
    private readonly IConsensusModule<Command, Result> _consensusModule;
    private readonly ILogger _logger;

    public SubmitCommandRequestHandler(IConsensusModule<Command, Result> consensusModule, IClusterInfo clusterInfo, ILogger logger)
    {
        ClusterInfo = clusterInfo;
        _consensusModule = consensusModule;
        _logger = logger;
    }
    
    public async Task HandleRequestAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken token)
    {
        response.KeepAlive = false;
        response.ContentType = "application/json";
        
        if (_consensusModule.CurrentRole != NodeRole.Leader)
        {
            _logger.Debug("Пришел запрос, но текущий узел не лидер");
            await RespondNotLeaderAsync(response);
            return;
        }
        
        if (!request.HasEntityBody)
        {
            _logger.Debug("В теле запроса не было данных");
            await RespondEmptyBodyNotAcceptedAsync(response);
            return;
        }
        
        _logger.Debug("Читаю строку запроса клиента");
        var requestString = await ReadRequestStringAsync(request);

        if (string.IsNullOrWhiteSpace(requestString))
        {
            _logger.Debug("В теле запроса не было данных");
            await RespondEmptyBodyNotAcceptedAsync(response);
            return;
        }

        if (TrySerializeCommandPayload(requestString, out var payload))
        {
            var submitResponse = _consensusModule.Handle(new SubmitRequest<Command>(payload));
            if (submitResponse.WasLeader)
            {
                RespondSuccessAsync(response, submitResponse);
                return;
            }
            
            await RespondNotLeaderAsync(response);
            return;
        }

        await RespondInvalidRequestStringAsync(response);
    }

    private async Task RespondInvalidRequestStringAsync(HttpListenerResponse response)
    {
        var body = SerializeResponse(false, "Ошибка десериализации команды");
        await using var writer = new StreamWriter(response.OutputStream);
        await writer.WriteAsync(body);
        await writer.FlushAsync();
    }
    
    private bool TrySerializeCommandPayload(string requestString, out CommandDescriptor<Command> command)
    {
        if (string.IsNullOrWhiteSpace(requestString))
        {
            command = default;
            return false;
        }
        var tokens = requestString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        // Точно будет хотя бы 1 элемент, т.к. проверяли выше, что строка не пуста
        var commandString = tokens[0];
        if (commandString.Equals("enqueue", StringComparison.InvariantCultureIgnoreCase) 
         && tokens.Length == 3)
        {
            var key = int.Parse(tokens[1]);
            var data = Encoding.GetBytes( tokens[2] );
            command = new CommandDescriptor<Command>( new EnqueueCommand(key, data), false );
            return true;
        }

        if (commandString.Equals("dequeue", StringComparison.InvariantCultureIgnoreCase) 
         && tokens.Length == 1)
        {
            command = new CommandDescriptor<Command>( DequeueCommand.Instance, false );
            return true;
        }

        if (commandString.Equals("count", StringComparison.InvariantCultureIgnoreCase) 
         && tokens.Length == 1)
        {
            command = new CommandDescriptor<Command>( CountCommand.Instance, true );
            return true;
        }

        command = default;
        return false;
    }

    private void RespondSuccessAsync(HttpListenerResponse httpResponse,
                                     SubmitResponse<Result> submitResponse)
    {

        Dictionary<string, object?> resultData;
        HttpStatusCode responseStatus;
        bool success;

        if (submitResponse.TryGetResponse(out var result))
        {
            var visitor = new HttpResponseJobQueueResponseVisitor();
            result.Accept(visitor);
            
            success = true;
            resultData = visitor.Payload;
            responseStatus = HttpStatusCode.OK;
        }
        else if (submitResponse.WasLeader)
        {
            responseStatus = HttpStatusCode.InternalServerError;
            resultData = new Dictionary<string, object?>()
            {
                {"message", "Операция не вернула результат"}
            };
            success = false;
        }
        else
        {
            responseStatus = HttpStatusCode.TemporaryRedirect;
            resultData = new Dictionary<string, object?>()
            {
                {"message", "Узел не лидер"}
            };
            success = false;
        }
        
        httpResponse.StatusCode = ( int ) responseStatus;
        using var writer = new StreamWriter(httpResponse.OutputStream, leaveOpen: true);
        writer.Write(SerializeResponse(success, resultData));
    }

    private class HttpResponseJobQueueResponseVisitor : IResultVisitor
    {
        public Dictionary<string, object?> Payload { get; private set; } = new();

        public void Visit(DequeueResult response)
        {
            Payload["type"] = "dequeue";
            if (response.Success)
            {
                Payload["ok"] = true;
                Payload["key"] = response.Key;
                Payload["data"] = Convert.ToBase64String(response.Payload);
            }
            else
            {
                Payload["ok"] = false;
            }
        }

        public void Visit(EnqueueResult response)
        {
            Payload["type"] = "enqueue";
            Payload["ok"] = response.Success;
        }

        public void Visit(CountResult response)
        {
            Payload["count"] = response.Count;
        }
    }
    
    private async Task RespondNotLeaderAsync(HttpListenerResponse response)
    {
        response.StatusCode = ( int ) HttpStatusCode.MisdirectedRequest;
        await using var writer = new StreamWriter(response.OutputStream, Encoding);
        await writer.WriteAsync(SerializeResponse(false, new Dictionary<string, object?>()
        {
            {"message", "Текущий узел не лидер"},
            {"leaderId", ClusterInfo.LeaderId.Value}
        }));
    }
    

    private async Task RespondEmptyBodyNotAcceptedAsync(HttpListenerResponse response)
    {
        await using var writer = new StreamWriter(response.OutputStream, Encoding, leaveOpen: true);
        response.StatusCode = (int)HttpStatusCode.UnprocessableEntity;
        await writer.WriteAsync(SerializeResponse(false, "В теле запроса не указаны данные"));
    }

    private async Task<string> ReadRequestStringAsync(HttpListenerRequest request)
    {
        using var reader = new StreamReader(request.InputStream, leaveOpen: true);
        return await reader.ReadToEndAsync();
    }

    private string SerializeResponse(bool success, string? message)
    {
        return SerializeResponse(success, new Dictionary<string, object?>() {{"message", message}});
    }

    private string SerializeResponse(bool success, Dictionary<string, object?>? payload)
    {
        return JsonSerializer.Serialize(new Response()
        {
            Success = success,
            Payload = payload
        }, new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });
    }


    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
    private class Response
    {
        public bool Success { get; set; }
        public Dictionary<string, object?>? Payload { get; set; }
    }
}