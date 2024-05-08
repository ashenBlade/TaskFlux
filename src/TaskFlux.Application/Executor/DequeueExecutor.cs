using System.Diagnostics;
using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Core.Commands;
using TaskFlux.Core.Commands.Dequeue;
using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.PolicyViolation;
using TaskFlux.Core.Queue;

namespace TaskFlux.Application.Executor;

public class DequeueExecutor
{
    private readonly IRequestAcceptor _requestAcceptor;
    private readonly QueueName _queue;
    private readonly TimeSpan _timeout;

    /// <summary>
    /// Результат выполнения предыдущей операции.
    /// Если null, то запуска еще не производилось
    /// </summary>
    private (QueueRecord? Record, ErrorResponse? Error, PolicyViolationResponse? PolicyViolation, bool WasLeader)?
        _result = null;

    public DequeueExecutor(IRequestAcceptor requestAcceptor, QueueName queue, TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout,
                "Таймаут ожидания не может быть отрицательным");
        }

        _requestAcceptor = requestAcceptor;
        _queue = queue;
        _timeout = timeout;
    }

    public async Task PerformDequeueAsync(CancellationToken token)
    {
        var submitResult = await DoDequeueCommandAsync(token);

        if (submitResult.TryGetResponse(out var response) is false)
        {
            _result = (null, null, null, false);
            return;
        }

        if (response.Type is ResponseType.Error)
        {
            _result = (null, (ErrorResponse)response, null, true);
            return;
        }

        if (response.Type is ResponseType.PolicyViolation)
        {
            _result = (null, null, (PolicyViolationResponse)response, true);
            return;
        }

        if (await TryExtractRecordAsync(response, token) is { } record)
        {
            _result = (record, null, null, true);
            return;
        }

        _result = (null, null, null, true);
    }

    public bool TryGetRecord(out QueueRecord record)
    {
        if (_result is ({ } r, _, _, true))
        {
            record = r;
            return true;
        }

        record = default;
        return false;
    }

    public bool IsEmptyResult()
    {
        return _result is (null, _, _, true);
    }

    public bool TryGetPolicyViolation(out PolicyViolationResponse policyViolationResponse)
    {
        if (_result is (_, _, { } pv, true))
        {
            policyViolationResponse = pv;
            return true;
        }

        policyViolationResponse = default!;
        return false;
    }

    public bool TryGetError(out ErrorResponse errorResponse)
    {
        if (_result is (_, { } er, _, true))
        {
            errorResponse = er;
            return true;
        }

        errorResponse = default!;
        return false;
    }

    public bool WasLeader()
    {
        return _result is (_, _, _, true);
    }

    /// <summary>
    /// Закоммитить чтение записи - удалить ее из очереди и сохранить это состояние.
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns><c>true</c> - запись закоммичена успешно, <c>false</c> - узел перестал быть лидером и команда не выполнилась</returns>
    public async Task<bool> TryAckAsync(CancellationToken token)
    {
        return await DoAckCommon(true, token);
    }

    /// <summary>
    /// Вернуть запись обратно в очередь, если пользователь негативно подтвердил обработку сообщения
    /// </summary>
    /// <param name="token">Токен отмены</param>
    /// <returns><c>true</c> - запись успешно возвращена в очередь, <c>false</c> - узел перестал быть лидером и команда не выполнилась</returns>
    public async Task<bool> TryNackAsync(CancellationToken token)
    {
        return await DoAckCommon(false, token);
    }

    private async Task<bool> DoAckCommon(bool ack, CancellationToken token)
    {
        Debug.Assert(_result is not null, "_result is not null",
            "NACK должен быть вызван, только если запись была прочитана");
        if (_result is ({ } record, _, _, true))
        {
            Command command = ack
                ? new CommitDequeueCommand(DequeueResponse.CreatePersistent(_queue, record))
                : new ReturnRecordCommand(_queue, record);
            var r = await _requestAcceptor.AcceptAsync(command, token);
            return r.WasLeader;
        }

        _result = null;
        return false;
    }

    private async Task<SubmitResponse<Response>> DoDequeueCommandAsync(CancellationToken token)
    {
        SubmitResponse<Response> submitResult;
        if (_timeout == TimeSpan.Zero)
        {
            submitResult =
                await _requestAcceptor.AcceptAsync(ImmediateDequeueCommand.CreateNonPersistent(_queue), token);
        }
        else
        {
            submitResult =
                await _requestAcceptor.AcceptAsync(AwaitableDequeueCommand.CreateNonPersistent(_queue, _timeout),
                    token);
        }

        _result = null;
        return submitResult;
    }

    /// <summary>
    /// Метод для получения прочитанной записи из ответа команды.
    /// Покрывает 2 ситуации:
    /// - Уже существующий ответ <br/>
    /// - Подписка <br/>
    /// </summary>
    /// <param name="response">Полученный ответ от команды чтения</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Прочитанная запись и очередь, из которой прочитали, либо <c>null</c> если ничего не прочитано</returns>
    private async ValueTask<QueueRecord?> TryExtractRecordAsync(
        Response response,
        CancellationToken token)
    {
        Debug.Assert(response.Type is ResponseType.Dequeue or ResponseType.Subscription,
            "response.Type is ResponseType.Dequeue or ResponseType.Subscription");

        // Результат операции получен сразу - возвращаем
        if (response.Type is ResponseType.Dequeue)
        {
            var immediateDequeueResponse = (DequeueResponse)response;
            return immediateDequeueResponse.TryGetResult(out _, out var record)
                ? record
                : null;
        }

        var subscriptionResponse = (QueueSubscriberResponse)response;

        // Необходимо использовать using (точнее, вызывать Dispose), чтобы корректно вернуть подписчика обратно в пул
        using var subscriber = subscriptionResponse.Subscriber;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(subscriptionResponse.Timeout);
        try
        {
            return await subscriber.WaitRecordAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
        }

        return null;
    }
}