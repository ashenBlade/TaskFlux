using System.Diagnostics;
using TaskQueue.Core.Policies;

namespace TaskQueue.Core;

public class Result<T> : Result
{
    /// <summary>
    /// Результат успешно выполненной операции
    /// </summary>
    private readonly T? _result;

    /// <summary>
    /// Получить результат успешно выполненной операции
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <see cref="Result{T}"/> хранил объект нарушенной политики (результата операции нет)
    /// </exception>
    public T SuccessResult =>
        _result
     ?? throw new InvalidOperationException("Result содержит нарушенную политику. Нельзя получить успешный результат");

    private Result(T? result, QueuePolicy? violatedPolicy) : base(violatedPolicy)
    {
        Debug.Assert(result is null ^ violatedPolicy is null,
            "result is null ^ violatedPolicy is null",
            "Result должен быть инициализирован только 1 не null значением.\nResult: {0}\nViolatedPolicy: {1}", result,
            violatedPolicy);

        _result = result;
    }

    /// <summary>
    /// Получить результат успешно выполненной операции (если есть)
    /// </summary>
    /// <param name="result">Результат операции</param>
    /// <returns>
    /// <c>true</c> - операция была успешно выполнена и результат есть,
    /// <c>false</c> - операция завершилась ошибкой (нарушение политики)
    /// </returns>
    public bool TryGetResult(out T result)
    {
        result = _result!;
        return _result is not null;
    }

    public static Result<T> Success(T value)
    {
        Debug.Assert(value is not null,
            "value is not null",
            "Результат операции не может быть null");

        return new Result<T>(value, null);
    }
}

public class Result
{
    /// <summary>
    /// Успешно ли выполнена операция
    /// </summary>
    public bool IsSuccess => _violatedPolicy is null;

    private readonly QueuePolicy? _violatedPolicy;

    protected Result(QueuePolicy? violatedPolicy)
    {
        _violatedPolicy = violatedPolicy;
    }

    public bool TryGetResult()
    {
        return _violatedPolicy is null;
    }

    public bool TryGetViolatedPolicy(out QueuePolicy violatedPolicy)
    {
        violatedPolicy = _violatedPolicy!;
        return _violatedPolicy is not null;
    }

    private static readonly Result SuccessResult = new(null);

    public static Result Success()
    {
        return SuccessResult;
    }

    public static Result PolicyViolation(QueuePolicy violatedPolicy)
    {
        Debug.Assert(violatedPolicy is not null,
            "violatedPolicy is not null",
            "Объект нарушенной политики должен быть указан");

        return new Result(violatedPolicy);
    }
}