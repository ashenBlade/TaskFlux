using System.Diagnostics;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Taskflux;
using TaskFlux.Application;
using TaskFlux.Application.Executor;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Core.Commands.Enqueue;
using DequeueResponse = Taskflux.DequeueResponse;
using EnqueueResponse = Taskflux.EnqueueResponse;

namespace TaskFlux.Transport.Grpc;

public class TaskFluxService : Taskflux.TaskFluxService.TaskFluxServiceBase
{
    private static readonly ErrorResponse InvalidQueueNameErrorResponse =
        new()
        {
            Code = ErrorCode.InvalidQueueName,
            Message = "Ошибка парсинга названия очереди"
        };

    private readonly IRequestAcceptor _requestAcceptor;
    private readonly IApplicationInfo _applicationInfo;
    private readonly ILogger<TaskFluxService> _logger;

    public TaskFluxService(
        IRequestAcceptor requestAcceptor,
        IApplicationInfo applicationInfo,
        ILogger<TaskFluxService> logger)
    {
        _requestAcceptor = requestAcceptor;
        _applicationInfo = applicationInfo;
        _logger = logger;
    }

    public override async Task Enqueue(IAsyncStreamReader<EnqueueRequest> requestStream,
        IServerStreamWriter<EnqueueResponse> responseStream, ServerCallContext context)
    {
        var token = context.CancellationToken;

        /*
         * Для реализации протокола подтверждений используем следующий хак:
         * 1. Каждый запрос может хранить в себе либо запрос, либо его подтверждение
         * 2. В переменной record храним отправленную пользователем запись:
         *  - Если переменная не null - получаем подтверждение и отправляем команду вставки
         *  - Иначе, ожидаем новой команды вставки
         */
        (QueueName queue, QueueRecord record)? record = null;
        await foreach (var enqueueRequest in requestStream.ReadAllAsync(token))
        {
            if (record is null)
            {
                // TODO: Dockerfile исправить

                // Это должна быть команда вставки
                var data = GetEnqueueRequestData(enqueueRequest);
                if (!QueueNameParser.TryParse(data.Queue, out var queueName))
                {
                    await responseStream.WriteAsync(new EnqueueResponse()
                    {
                        Error = InvalidQueueNameErrorResponse
                    }, token);
                    continue;
                }

                record = (queueName, data.Record);
                await responseStream.WriteAsync(new EnqueueResponse()
                {
                    Data = new EnqueueResponse.Types.EnqueueResponseData()
                    {
                        PolicyViolation = null
                    }
                }, token);
            }
            else
            {
                // Получаем подтверждение и выполняем команду
                Debug.Assert(record is not null, "record is not null");

                var ack = GetAck(enqueueRequest);
                if (!ack.Ack)
                {
                    // Клиент не захотел выполнять операцию
                    record = null;
                    await responseStream.WriteAsync(new EnqueueResponse()
                    {
                        Data = new EnqueueResponse.Types.EnqueueResponseData()
                        {
                            PolicyViolation = null
                        }
                    }, token);
                    continue;
                }

                var (queueName, r) = record.Value;
                var command = new EnqueueCommand(r.Priority, r.Payload.ToByteArray(), queueName);
                var result = await _requestAcceptor.AcceptAsync(command, token);

                ErrorResponse? error;
                EnqueueResponse.Types.EnqueueResponseData? enqueueResponseData;
                NotLeaderResponse? notLeaderResponse;

                if (result.TryGetResponse(out var response))
                {
                    notLeaderResponse = null;

                    switch (response.Type)
                    {
                        case ResponseType.Enqueue:
                            // Никакой ошибки, вставка успешна
                            error = null;
                            enqueueResponseData = new EnqueueResponse.Types.EnqueueResponseData()
                            {
                                PolicyViolation = null
                            };
                            break;
                        case ResponseType.Error:
                            var errorResponse = (Core.Commands.Error.ErrorResponse)response;
                            error = new ErrorResponse()
                            {
                                Code = GrpcHelpers.MapGrpcErrorCode(errorResponse.ErrorType),
                                Message = errorResponse.Message
                            };
                            enqueueResponseData = new EnqueueResponse.Types.EnqueueResponseData()
                            {
                                PolicyViolation = null
                            };
                            break;
                        case ResponseType.PolicyViolation:
                            var policyResponse = (Core.Commands.PolicyViolation.PolicyViolationResponse)response;
                            error = null;

                            enqueueResponseData = new EnqueueResponse.Types.EnqueueResponseData()
                            {
                                PolicyViolation = GrpcHelpers.MapGrpcPolicyViolationResponse(policyResponse)
                            };
                            break;

                        case ResponseType.Ok:
                        case ResponseType.Dequeue:
                        case ResponseType.Subscription:
                        case ResponseType.ListQueues:
                        case ResponseType.Count:
                        case ResponseType.CreateQueue:
                        case ResponseType.DeleteQueue:
                            Debug.Assert(false, "false", "Команда вставки записи вернула неизвестный ответ");
                            throw new ArgumentOutOfRangeException(nameof(response.Type), response.Type,
                                "Неожиданный тип ответа команды");
                        default:
                            Debug.Assert(Enum.IsDefined(response.Type), "Enum.IsDefined(response.Type)",
                                "Неизвестный тип ответа команды");
                            Debug.Assert(false, "false", "Необработанный тип ответа");
                            throw new ArgumentOutOfRangeException(nameof(response.Type), response.Type,
                                "Неизвестный тип ответа команды");
                    }
                }
                else
                {
                    error = null;
                    enqueueResponseData = null;
                    notLeaderResponse = new NotLeaderResponse()
                    {
                        LeaderId = _applicationInfo.LeaderId?.Id ??
                                   0 /* Nullable int почему-то не генерируется, приходится отправлять 0 */
                    };
                }

                await responseStream.WriteAsync(new EnqueueResponse()
                {
                    Data = enqueueResponseData,
                    Error = error,
                    NotLeader = notLeaderResponse,
                }, token);
                record = null;
            }
        }

        return;

        static EnqueueRequest.Types.EnqueueRequestData GetEnqueueRequestData(EnqueueRequest request)
        {
            if (request.ResultCase != EnqueueRequest.ResultOneofCase.Data)
            {
                throw new RpcException(new Status(StatusCode.FailedPrecondition, "Ожидался запрос типа Data"));
            }

            return request.Data;
        }

        static AckMessage GetAck(EnqueueRequest request)
        {
            if (request.ResultCase != EnqueueRequest.ResultOneofCase.Data)
            {
                throw new RpcException(new Status(StatusCode.FailedPrecondition, "Ожидался запрос типа Data"));
            }

            return request.Ack;
        }
    }

    public override async Task Dequeue(IAsyncStreamReader<DequeueRequest> requestStream,
        IServerStreamWriter<DequeueResponse> responseStream, ServerCallContext context)
    {
        var token = context.CancellationToken;

        /*
         * Логика работы аналогична логике вставки:
         * - Храним запись, с которой работаем в отдельной переменной
         * - В зависимости от того пуста или нет переменная выполняем действия:
         *   - null - Ожидаем новую команду чтения
         *   - != null - Ожидаем подтверждение и выполняем его
         */
        DequeueExecutor? executor = null;
        await foreach (var dequeueRequest in requestStream.ReadAllAsync(token))
        {
            if (executor is null)
            {
                var data = GetDequeueRequestData(dequeueRequest);
                if (!QueueNameParser.TryParse(data.Queue, out var queueName))
                {
                    await responseStream.WriteAsync(new DequeueResponse()
                    {
                        Error = InvalidQueueNameErrorResponse
                    }, token);
                    continue;
                }

                var exec = new DequeueExecutor(_requestAcceptor, queueName, GetTimeout(data.Timeout));
                await exec.PerformDequeueAsync(token);

                DequeueResponse.Types.DequeueResponseData? dequeueResponseData;
                ErrorResponse? errorResponse;
                PolicyViolationResponse? policyViolationResponse;
                NotLeaderResponse? notLeader;

                if (exec.TryGetRecord(out var record))
                {
                    dequeueResponseData = new DequeueResponse.Types.DequeueResponseData()
                    {
                        Record = new QueueRecord()
                        {
                            Priority = record.Priority,
                            Payload = ByteString.CopyFrom(record.Payload.AsSpan())
                        }
                    };
                    errorResponse = null;
                    policyViolationResponse = null;
                    notLeader = null;
                    executor = exec;
                }
                else if (exec.IsEmptyResult())
                {
                    dequeueResponseData = new DequeueResponse.Types.DequeueResponseData()
                    {
                        Record = null
                    };
                    errorResponse = null;
                    policyViolationResponse = null;
                    notLeader = null;
                }
                else if (exec.TryGetPolicyViolation(out var policyViolation))
                {
                    policyViolationResponse = GrpcHelpers.MapGrpcPolicyViolationResponse(policyViolation);
                    dequeueResponseData = null;
                    errorResponse = null;
                    notLeader = null;
                }
                else if (exec.TryGetError(out var error))
                {
                    errorResponse = new ErrorResponse()
                    {
                        Code = GrpcHelpers.MapGrpcErrorCode(error.ErrorType),
                        Message = error.Message
                    };
                    policyViolationResponse = null;
                    dequeueResponseData = null;
                    notLeader = null;
                }
                else if (!exec.WasLeader())
                {
                    notLeader = new NotLeaderResponse()
                    {
                        LeaderId = GetGrpcLeaderId()
                    };
                    policyViolationResponse = null;
                    errorResponse = null;
                    dequeueResponseData = null;
                }
                else
                {
                    Debug.Assert(false, "false", "Неизвестный результат работы dequeue исполнителя");
                    throw new InvalidOperationException("Неизвестный результат работы исполнителя dequeue");
                }

                await responseStream.WriteAsync(new DequeueResponse()
                {
                    Error = errorResponse,
                    NotLeader = notLeader,
                    Success = dequeueResponseData,
                    PolicyViolation = policyViolationResponse,
                }, token);
            }
            else
            {
                Task<bool> ackOperation;
                if (IsAck(dequeueRequest))
                {
                    ackOperation = executor.TryAckAsync(token);
                }
                else
                {
                    ackOperation = executor.TryNackAsync(token);
                }

                DequeueResponse response;
                if (await ackOperation)
                {
                    response = new DequeueResponse()
                    {
                        Success = new DequeueResponse.Types.DequeueResponseData()
                        {
                            Record = null
                        }
                    };
                }
                else
                {
                    response = new DequeueResponse()
                    {
                        NotLeader = new NotLeaderResponse()
                        {
                            LeaderId = GetGrpcLeaderId()
                        }
                    };
                }

                await responseStream.WriteAsync(response, token);
                executor = null;
            }
        }

        if (executor is not null)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
            await executor.TryNackAsync(cts.Token);
        }

        return;

        static DequeueRequest.Types.DequeueRequestData GetDequeueRequestData(DequeueRequest r)
        {
            if (r.OperationCase != DequeueRequest.OperationOneofCase.Request)
            {
                throw new RpcException(new Status(StatusCode.FailedPrecondition,
                    "Неожиданный тип запроса: ожидался Dequeue"));
            }

            return r.Request;
        }

        static bool IsAck(DequeueRequest r)
        {
            if (r.OperationCase != DequeueRequest.OperationOneofCase.Ack)
            {
                throw new RpcException(new Status(StatusCode.FailedPrecondition,
                    "Неожиданный тип запроса: ожидался Ack"));
            }

            return r.Ack.Ack;
        }
    }

    private int GetGrpcLeaderId()
    {
        return _applicationInfo.LeaderId?.Id ?? 0;
    }

    private static TimeSpan GetTimeout(int timeout)
    {
        if (timeout == -1)
        {
            return Timeout.InfiniteTimeSpan;
        }

        if (timeout < -1)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition,
                "Таймаут ожидания не может быть меньше -1"));
        }

        if (timeout == 0)
        {
            return TimeSpan.Zero;
        }

        return TimeSpan.FromMilliseconds(timeout);
    }

    /*
     * TODO:
     * - Create Queue
     * - Delete Queue
     * - Get Count
     *
     * Документация, тесты на GRPC (надо ли?)
     */
}