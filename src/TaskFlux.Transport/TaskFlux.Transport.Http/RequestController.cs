using System.Text;
using Microsoft.AspNetCore.Mvc;
using TaskFlux.Application;
using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Core.Commands.Count;
using TaskFlux.Core.Commands.CreateQueue;
using TaskFlux.Core.Commands.DeleteQueue;
using TaskFlux.Core.Commands.Dequeue;
using TaskFlux.Core.Commands.Enqueue;
using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.ListQueues;
using TaskFlux.Core.Commands.Ok;
using TaskFlux.Core.Commands.PolicyViolation;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Transport.Http.Dto;

namespace TaskFlux.Transport.Http;

[ApiController]
[Route("commands/v1/")]
public class RequestController(
    IRequestAcceptor requestAcceptor,
    IApplicationInfo applicationInfo)
    : ControllerBase
{
    private static readonly Encoding Encoding = Encoding.UTF8;

    [HttpPost("enqueue")]
    public async Task<ActionResult<CommandResponseDto>> Enqueue(
        [FromBody] EnqueueRequestDto request,
        CancellationToken token)
    {
        if (!QueueNameParser.TryParse(request.Queue, out var queueName))
        {
            return GetInvalidQueueNameResult();
        }

        var payloadEncoded = Encoding.GetBytes(request.Payload);

        var command = new EnqueueCommand(request.Key, payloadEncoded, queueName);
        return await HandleCommandCoreAsync(command, token);
    }

    private BadRequestObjectResult GetInvalidQueueNameResult()
    {
        return BadRequest(new CommandResponseDto() { Success = false, Error = "Передано невалидное название очереди" });
    }


    [HttpPost("dequeue")]
    public async Task<ActionResult<CommandResponseDto>> Dequeue(
        [FromBody] DequeueRequestDto dto,
        CancellationToken token)
    {
        if (!QueueNameParser.TryParse(dto.Queue, out var queueName))
        {
            return GetInvalidQueueNameResult();
        }

        return await HandleCommandCoreAsync(
            ImmediateDequeueCommand.CreatePersistent(queueName), // Сразу сохраняем результат операции  
            token);
    }

    [HttpPost("count")]
    public async Task<ActionResult<CommandResponseDto>> Count([FromBody] CountRequestDto dto, CancellationToken token)
    {
        if (!QueueNameParser.TryParse(dto.Queue, out var queueName))
        {
            return GetInvalidQueueNameResult();
        }

        return await HandleCommandCoreAsync(new CountCommand(queueName), token);
    }

    private async Task<ActionResult<CommandResponseDto>> HandleCommandCoreAsync(
        Command command,
        CancellationToken token)
    {
        Metrics.TotalConnectedClients.Add(1);
        Metrics.TotalAcceptedRequests.Add(1);
        try
        {
            var submitResponse = await requestAcceptor.AcceptAsync(command, token);
            var commandResponse = await TryGetResponseAsync(submitResponse, token);
            if (commandResponse is not null)
            {
                var visitor = new HttpResponseJobQueueResponseVisitor();
                commandResponse.Accept(visitor);
                return Ok(new CommandResponseDto() { Success = true, Payload = visitor.Payload });
            }

            return BadRequest(new CommandResponseDto()
            {
                Success = false,
                Error = submitResponse.WasLeader
                    ? "Неизвестная ошибка"
                    : "Узел не является лидером",
                LeaderId = applicationInfo.LeaderId?.Id
            });
        }
        finally
        {
            Metrics.TotalDisconnectedClients.Add(1);
            Metrics.TotalProcessedRequests.Add(1);
        }
    }

    private async ValueTask<Response?> TryGetResponseAsync(SubmitResponse<Response> firstResponse,
        CancellationToken token)
    {
        if (firstResponse.TryGetResponse(out var response))
        {
            if (response is DequeueResponse { Success: true } dr)
            {
                var commitResponse = await requestAcceptor.AcceptAsync(new CommitDequeueCommand(dr), token);
                return commitResponse.HasValue
                    ? dr
                    : null;
            }

            return response;
        }

        return null;
    }

    private class HttpResponseJobQueueResponseVisitor : IResponseVisitor
    {
        public Dictionary<string, object?> Payload { get; private set; } = new();

        public void Visit(DequeueResponse response)
        {
            Payload["type"] = "dequeue";
            if (response.TryGetResult(out var queueName, out var record))
            {
                Payload["ok"] = true;
                Payload["queue"] = queueName.Name;
                Payload["id"] = record.Id.Id;
                Payload["priority"] = record.Priority;
                Payload["data"] = Convert.ToBase64String(record.Payload);
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
            Payload["subtype"] = (byte)response.ErrorType;
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
                { "count", m.Count },
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

        public void Visit(QueueSubscriberResponse response)
        {
            throw new InvalidOperationException("HTTP интерфейс пока не поддерживает возможность подписки");
        }
    }
}