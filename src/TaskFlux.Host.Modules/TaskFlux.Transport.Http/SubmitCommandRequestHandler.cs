using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Consensus.Core.Submit;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Commands.Visitors;
using TaskFlux.Models;
using TaskFlux.Transport.Common;

namespace TaskFlux.Transport.Http;

public class SubmitCommandRequestHandler : IRequestHandler
{
    private static readonly Encoding Encoding = Encoding.UTF8;

    private readonly IApplicationInfo _applicationInfo;
    private readonly IRequestAcceptor _requestAcceptor;
    private readonly ILogger _logger;

    public SubmitCommandRequestHandler(IRequestAcceptor requestAcceptor,
                                       IApplicationInfo applicationInfo,
                                       ILogger logger)
    {
        _applicationInfo = applicationInfo;
        _logger = logger;
        _requestAcceptor = requestAcceptor;
    }

    public async Task HandleRequestAsync(HttpListenerRequest request,
                                         HttpListenerResponse response,
                                         CancellationToken token)
    {
        response.KeepAlive = false;
        response.ContentType = "application/json";

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

        if (TrySerializeCommandPayload(requestString, out var command))
        {
            var submitResponse = await _requestAcceptor.AcceptAsync(command, token);
            await RespondAsync(response, submitResponse);
        }
        else
        {
            await RespondInvalidRequestStringAsync(response);
        }
    }

    private async Task RespondInvalidRequestStringAsync(HttpListenerResponse response)
    {
        var body = SerializeResponse(false, "Ошибка десериализации команды");
        await using var writer = new StreamWriter(response.OutputStream);
        await writer.WriteAsync(body);
        await writer.FlushAsync();
    }

    private bool TrySerializeCommandPayload(string requestString, out Command command)
    {
        if (string.IsNullOrWhiteSpace(requestString))
        {
            command = default!;
            return false;
        }

        var tokens = requestString.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        // Точно будет хотя бы 1 элемент, т.к. проверяли выше, что строка не пуста
        var commandString = tokens[0];
        if (commandString.Equals("enqueue", StringComparison.InvariantCultureIgnoreCase)
         && tokens.Length == 3)
        {
            var key = int.Parse(tokens[1]);
            var data = Encoding.GetBytes(tokens[2]);
            command = new EnqueueCommand(key, data, QueueName.Default);
            return true;
        }

        if (commandString.Equals("dequeue", StringComparison.InvariantCultureIgnoreCase)
         && tokens.Length == 1)
        {
            command = new DequeueRecordCommand(QueueName.Default,
                permanent: true); // Коммитим команду сразу, т.к. в HTTP не будет времени думать
            return true;
        }

        if (commandString.Equals("count", StringComparison.InvariantCultureIgnoreCase)
         && tokens.Length == 1)
        {
            command = new CountCommand(QueueName.Default);
            return true;
        }

        command = default!;
        return false;
    }

    private async Task RespondAsync(HttpListenerResponse httpResponse,
                                    SubmitResponse<Commands.Response> submitResponse)
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
            resultData = new Dictionary<string, object?>() {{"message", "Операция не вернула результат"}};
            success = false;
        }
        else
        {
            responseStatus = HttpStatusCode.TemporaryRedirect;
            resultData = new Dictionary<string, object?>()
            {
                {"message", "Узел не лидер"}, {"leaderId", _applicationInfo.LeaderId?.ToString() ?? "-"}
            };
            success = false;
        }

        httpResponse.StatusCode = ( int ) responseStatus;
        await using var writer = new StreamWriter(httpResponse.OutputStream, leaveOpen: true);
        await writer.WriteAsync(SerializeResponse(success, resultData));
    }

    private class HttpResponseJobQueueResponseVisitor : IResponseVisitor
    {
        public Dictionary<string, object?> Payload { get; private set; } = new();

        public void Visit(DequeueResponse response)
        {
            Payload["type"] = "dequeue";
            if (response.TryGetResult(out var queueName, out var key, out var message))
            {
                Payload["ok"] = true;
                Payload["key"] = key;
                Payload["queue"] = queueName.Name;
                Payload["data"] = Convert.ToBase64String(message);
            }
            else
            {
                Payload["ok"] = false;
            }
        }

        public void Visit(EnqueueResponse response)
        {
            Payload["type"] = "enqueue";
            Payload["ok"] = true;
        }

        public void Visit(CreateQueueResponse response)
        {
            Payload["type"] = "create-queue";
            Payload["ok"] = true;
        }

        public void Visit(DeleteQueueResponse response)
        {
            Payload["type"] = "delete-queue";
            Payload["ok"] = true;
        }

        public void Visit(CountResponse response)
        {
            Payload["count"] = response.Count;
        }

        public void Visit(ErrorResponse response)
        {
            Payload["type"] = "error";
            Payload["subtype"] = ( byte ) response.ErrorType;
            Payload["message"] = response.Message;
        }

        public void Visit(OkResponse response)
        {
            Payload["type"] = "ok";
        }

        public void Visit(ListQueuesResponse response)
        {
            Payload["type"] = "list-queues";
            Payload["data"] = response.Metadata.ToDictionary(m => m.QueueName, m => new Dictionary<string, object?>()
            {
                {"count", m.Count},
                {
                    "limit", m.HasMaxSize
                                 ? m.MaxQueueSize
                                 : null
                }
            });
        }

        public void Visit(PolicyViolationResponse response)
        {
            Payload["type"] = "ok";
        }
    }


    private async Task RespondEmptyBodyNotAcceptedAsync(HttpListenerResponse response)
    {
        await using var writer = new StreamWriter(response.OutputStream, Encoding, leaveOpen: true);
        response.StatusCode = ( int ) HttpStatusCode.UnprocessableEntity;
        await writer.WriteAsync(SerializeResponse(false, "В теле запроса не указаны данные"));
    }

    private static async Task<string> ReadRequestStringAsync(HttpListenerRequest request)
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
        return JsonSerializer.Serialize(new Response() {Success = success, Payload = payload},
            new JsonSerializerOptions(JsonSerializerDefaults.Web)
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