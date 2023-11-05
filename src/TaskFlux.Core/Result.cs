using System.Diagnostics;

namespace TaskFlux.Core;

public sealed class Result<T> : Result
{
    /// <summary>
    /// Результат успешно выполненной операции
    /// </summary>
    private readonly T _result;

    /// <summary>
    /// Получить результат успешно выполненной операции
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <see cref="Result{T}"/> хранил объект нарушенной политики (результата операции нет)
    /// </exception>
    public T SuccessResult
    {
        get
        {
            if (IsSuccess)
            {
                return _result;
            }

            throw new InvalidOperationException(
                "Result содержит нарушенную политику. Нельзя получить успешный результат");
        }
    }

    private Result(T result, QueuePolicy? violatedPolicy)
        : base(violatedPolicy)
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
        return new Result<T>(value, null);
    }

    public new static Result<T> PolicyViolation(QueuePolicy policy)
    {
        Debug.Assert(policy is not null,
            "policy is not null",
            "Объект политики должен быть указан");
        ArgumentNullException.ThrowIfNull(policy);

        return new Result<T>(default!, policy);
    }
}

public class Result
{
    /// <summary>
    /// Успешно ли выполнена операция
    /// </summary>
    public bool IsSuccess => _violatedPolicy is null;

    /// <summary>
    /// Политика, которая была нарушена в результате выполнения команды
    /// </summary>
    private readonly QueuePolicy? _violatedPolicy;

    /// <summary>
    /// Метод для получения объекта нарушенной политики 
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Результат представляет успешное выполнение (объекта нарушенной политики нет)
    /// </exception>
    public QueuePolicy ViolatedPolicy =>
        _violatedPolicy
     ?? throw new InvalidOperationException(
            "Нельзя получить объект нарушенной политики: результат выполнения успешный");


    internal Result(QueuePolicy? violatedPolicy)
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