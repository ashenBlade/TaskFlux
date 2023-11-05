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

    private Result(T? result, QueuePolicy? violatedPolicy, ErrorCode? code)
        : base(violatedPolicy, code)
    {
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

        return new Result<T>(value, null, null);
    }

    public new static Result<T> PolicyViolation(QueuePolicy policy)
    {
        Debug.Assert(policy is not null,
            "policy is not null",
            "Объект политики должен быть указан");
        return new Result<T>(default, policy, null);
    }

    public new static Result<T> Error(ErrorCode code)
    {
        return new Result<T>(default, null, code);
    }
}

public class Result
{
    /// <summary>
    /// Успешно ли выполнена операция
    /// </summary>
    public bool IsSuccess => _violatedPolicy is null // Никакая политика не нарушена 
                          && _errorCode is null;     // Завершились без ошибки

    /// <summary>
    /// Политика, которая была нарушена в результате выполнения команды
    /// </summary>
    private readonly QueuePolicy? _violatedPolicy;

    /// <summary>
    /// Ошибка, возникшая в результате выполнения работы
    /// </summary>
    private readonly ErrorCode? _errorCode;

    protected Result(QueuePolicy? violatedPolicy, ErrorCode? errorCode)
    {
        _violatedPolicy = violatedPolicy;
        _errorCode = errorCode;
    }

    public bool TryGetResult()
    {
        return _violatedPolicy is null;
    }

    public bool TryGetErrorCode(out ErrorCode code)
    {
        if (_errorCode is { } c)
        {
            code = c;
            return true;
        }

        code = default!;
        return false;
    }

    public bool TryGetViolatedPolicy(out QueuePolicy violatedPolicy)
    {
        violatedPolicy = _violatedPolicy!;
        return _violatedPolicy is not null;
    }

    private static readonly Result SuccessResult = new(null, null);

    public static Result Success()
    {
        return SuccessResult;
    }

    public static Result PolicyViolation(QueuePolicy violatedPolicy)
    {
        Debug.Assert(violatedPolicy is not null,
            "violatedPolicy is not null",
            "Объект нарушенной политики должен быть указан");

        return new Result(violatedPolicy, null);
    }

    public static Result Error(ErrorCode code)
    {
        return new Result(null, code);
    }
}