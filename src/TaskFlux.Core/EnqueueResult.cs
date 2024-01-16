using System.Diagnostics;
using TaskFlux.Core.Policies;

namespace TaskFlux.Core;

public sealed class EnqueueResult
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


    internal EnqueueResult(QueuePolicy? violatedPolicy)
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

    private static readonly EnqueueResult SuccessEnqueueResult = new(null);

    public static EnqueueResult Success()
    {
        return SuccessEnqueueResult;
    }

    public static EnqueueResult PolicyViolation(QueuePolicy violatedPolicy)
    {
        Debug.Assert(violatedPolicy is not null,
            "violatedPolicy is not null",
            "Объект нарушенной политики должен быть указан");

        return new EnqueueResult(violatedPolicy);
    }
}