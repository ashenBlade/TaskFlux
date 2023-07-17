using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Consensus.Core;
using Consensus.Core.Commands.Submit;
using Consensus.StateMachine.JobQueue.Commands;
using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.Error;
using Consensus.StateMachine.JobQueue.Commands.GetCount;
using Consensus.StateMachine.JobQueue.Serialization;
using Serilog;

namespace TaskFlux.Host.Modules.HttpRequest;

public class SubmitCommandRequestHandler: IRequestHandler
{
    private static readonly Encoding Encoding = Encoding.UTF8;
    private readonly IConsensusModule _consensusModule;
    private readonly IJobQueueRequestSerializer _requestSerializer;
    private readonly ILogger _logger;

    public SubmitCommandRequestHandler(IConsensusModule consensusModule, IJobQueueRequestSerializer requestSerializer, ILogger logger)
    {
        _consensusModule = consensusModule;
        _requestSerializer = requestSerializer;
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

        if (TrySerializeRequestPayload(requestString, out var payload))
        {
            var submitResponse = _consensusModule.Handle(new SubmitRequest(payload));
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
    
    private bool TrySerializeRequestPayload(string requestString, out byte[] payload)
    {
        var linesArray = SplitLines().ToArray();
        if (linesArray.Length == 0)
        {
            payload = Array.Empty<byte>();
            return false;
        }

        if (linesArray.Length == 1)
        {
            return TryDeserializeSingleRequest(linesArray[0], out payload);
        }

        return TryDeserializeBatchRequest(linesArray, out payload);

        IEnumerable<string> SplitLines()
        {
            var reader = new StringReader(requestString);
            while (reader.ReadLine() is {} line)
            {
                yield return line;
            }
        }
    }

    private bool TryDeserializeBatchRequest(string[] requestLines, out byte[] payload)
    {
        var requests = new IJobQueueRequest[requestLines.Length];
        for (var i = 0; i < requests.Length; i++)
        {
            if (!TryDeserializeRequest(requestLines[i], out var request))
            {
                payload = Array.Empty<byte>();
                return false;
            }

            requests[i] = request;
        }

        var batchRequest = new BatchRequest(requests);
        using var memory = new MemoryStream();
        using var writer = new BinaryWriter(memory);
        _requestSerializer.Serialize(batchRequest, writer);
        payload = memory.ToArray();
        return true;
    }

    private static bool TryDeserializeRequest(string requestString, out IJobQueueRequest request)
    {
        if (string.IsNullOrWhiteSpace(requestString))
        {
            request = null!;
            return false;
        }
        
        var tokens = requestString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        
        try
        {
            var command = tokens[0];
            IJobQueueRequest? parsedRequest = null;
            if (command.Equals("enqueue", StringComparison.InvariantCultureIgnoreCase) 
             && tokens.Length == 3)
            {
                var key = int.Parse(tokens[1]);
                var data = Encoding.GetBytes( tokens[2] );
                parsedRequest = new EnqueueRequest(key, data);
            }
            else if (command.Equals("dequeue", StringComparison.InvariantCultureIgnoreCase) 
                  && tokens.Length == 1)
            {
                parsedRequest = DequeueRequest.Instance;
            }
            else if (command.Equals("count", StringComparison.InvariantCultureIgnoreCase) 
                  && tokens.Length == 1)
            {
                parsedRequest = GetCountRequest.Instance;
            }

            request = parsedRequest!;
            return parsedRequest is not null;
        }
        catch (Exception)
        {
            request = null!;
            return false;
        }
    }

    private bool TryDeserializeSingleRequest(string requestString, out byte[] payload)
    {
        try
        {
            var tokens = requestString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            
            // Точно будет хотя бы 1 элемент, т.к. проверяли выше, что строка не пуста
            var command = tokens[0];
            IJobQueueRequest? request = null;
            if (command.Equals("enqueue", StringComparison.InvariantCultureIgnoreCase) 
             && tokens.Length == 3)
            {
                var key = int.Parse(tokens[1]);
                var data = Encoding.GetBytes( tokens[2] );
                request = new EnqueueRequest(key, data);
            }
            else if (command.Equals("dequeue", StringComparison.InvariantCultureIgnoreCase) 
                  && tokens.Length == 1)
            {
                request = DequeueRequest.Instance;
            }
            else if (command.Equals("count", StringComparison.InvariantCultureIgnoreCase) 
                  && tokens.Length == 1)
            {
                request = GetCountRequest.Instance;
            }

            if (request is null)
            {
                payload = Array.Empty<byte>();
                return false;
            }

            using var memory = new MemoryStream();
            using var writer = new BinaryWriter(memory);
            _requestSerializer.Serialize(request, writer);
            payload = memory.ToArray();
            return true;
        }
        catch (Exception)
        {
            payload = Array.Empty<byte>();
            return false;
        }
    }

    private void RespondSuccessAsync(HttpListenerResponse response,
                                     SubmitResponse submitResponse)
    {
        var visitor = VisitResponse();

        response.StatusCode = ( int ) ( visitor.Ok
                                            ? HttpStatusCode.OK
                                            : HttpStatusCode.BadRequest );

        using var writer = new StreamWriter(response.OutputStream, leaveOpen: true);

        writer.WriteAsync(SerializeResponse(visitor.Ok, visitor.Payload));

        HttpResponseJobQueueResponseVisitor VisitResponse()
        {
            using var memory = new MemoryStream();
            submitResponse.Response.WriteTo(memory);
            var defaultResponse = JobQueueResponseDeserializer.Instance.Deserialize(memory.ToArray());
            var v = new HttpResponseJobQueueResponseVisitor();
            defaultResponse.Accept(v);
            return v;
        }
    }

    private class HttpResponseJobQueueResponseVisitor : IJobQueueResponseVisitor
    {
        public Dictionary<string, object?> Payload { get; private set; } = new();
        public bool Ok { get; private set; } = true;
        
        private void SetError()
        {
            Ok = false;
        }
        
        public void Visit(DequeueResponse response)
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

        public void Visit(EnqueueResponse response)
        {
            Payload["type"] = "enqueue";
            Payload["ok"] = response.Success;
        }

        public void Visit(GetCountResponse response)
        {
            Payload["count"] = response.Count;
        }

        public void Visit(ErrorResponse response)
        {
            SetError();

            if (!string.IsNullOrWhiteSpace(response.Message))
            {
                Payload["message"] = response.Message;
            }
        }

        public void Visit(BatchResponse response)
        {
            Payload["type"] = "batch";
            Payload["count"] = response.Responses.Count;
            var data = new List<(Dictionary<string, object?> Payload, bool Ok)>();
            var oldPayload = Payload;
            var oldOk = Ok;
            foreach (var innerResponse in response.Responses)
            {
                Payload = new Dictionary<string, object?>();
                Ok = true;
                innerResponse.Accept(this);
                data.Add((Payload, Ok));
            }

            Payload = oldPayload;
            Ok = oldOk;
            Payload["data"] = data.Select(tuple => tuple.Payload);
        }
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