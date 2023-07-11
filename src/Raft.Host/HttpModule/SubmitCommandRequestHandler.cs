using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raft.Core.Commands.Submit;
using Raft.Core.Node;
using Raft.Host.Infrastructure;
using Raft.StateMachine.JobQueue.Commands;
using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.Error;
using Raft.StateMachine.JobQueue.Commands.GetCount;
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

        if (!_node.IsLeader())
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
            var submitResponse = _node.Handle(new SubmitRequest(payload));
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

    private static bool TrySerializeRequestPayload(string requestString, out byte[] payload)
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

            var visitor = new SerializerJobQueueRequestVisitor();
            request.Accept(visitor);
            payload = visitor.Payload;
            return true;
        }
        catch (Exception)
        {
            payload = Array.Empty<byte>();
            return false;
        }
    }

    private class SerializerJobQueueRequestVisitor : IJobQueueRequestVisitor
    {
        public byte[] Payload { get; private set; } = Array.Empty<byte>();
        
        public void Visit(DequeueRequest request)
        {
            using var memory = new MemoryStream();
            using var writer = new BinaryWriter(memory);
            JobQueueRequestSerializer.Instance.Serialize(request, writer);
            Payload = memory.ToArray();
        }

        public void Visit(EnqueueRequest request)
        {
            using var memory = new MemoryStream();
            using var writer = new BinaryWriter(memory);
            JobQueueRequestSerializer.Instance.Serialize(request, writer);
            Payload = memory.ToArray();
        }

        public void Visit(GetCountRequest request)
        {
            using var memory = new MemoryStream();
            using var writer = new BinaryWriter(memory);
            JobQueueRequestSerializer.Instance.Serialize(request, writer);
            Payload = memory.ToArray();
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
            var defaultResponse = DefaultResponseDeserializer.Instance.Deserialize(memory.ToArray());
            var v = new HttpResponseJobQueueResponseVisitor();
            defaultResponse.Accept(v);
            return v;
        }
    }

    private class HttpResponseJobQueueResponseVisitor : IJobQueueResponseVisitor
    {
        public Dictionary<string, object?> Payload { get; set; } = new();
        public bool Ok { get; set; } = true;
        
        private void SetError()
        {
            Ok = false;
        }
        
        public void Visit(DequeueResponse response)
        {
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