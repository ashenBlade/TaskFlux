using System.Diagnostics;
using TaskFlux.Core.Policies;
using TaskFlux.Domain;

namespace TaskFlux.Core;

/// <summary>
/// Результат операции вставки записи в очередь
/// </summary>
public sealed class EnqueueResult
{
    /// <summary>
    /// Политика, которая была нарушена в результате выполнения команды
    /// </summary>
    private readonly QueuePolicy? _violatedPolicy;

    /// <summary>
    /// Запись очереди если вставка успешна
    /// </summary>
    private readonly QueueRecord? _createdRecord;

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

    private EnqueueResult(QueuePolicy? violatedPolicy, QueueRecord? record)
    {
        _violatedPolicy = violatedPolicy;
        _createdRecord = record;
    }

    public bool TryGetRecord(out QueueRecord record)
    {
        if (_createdRecord is { } r)
        {
            record = r;
            return true;
        }

        record = default;
        return false;
    }

    public bool TryGetViolatedPolicy(out QueuePolicy violatedPolicy)
    {
        violatedPolicy = _violatedPolicy!;
        return _violatedPolicy is not null;
    }

    public static EnqueueResult Success(QueueRecord record)
    {
        return new EnqueueResult(null, record);
    }

    public static EnqueueResult PolicyViolation(QueuePolicy violatedPolicy)
    {
        Debug.Assert(violatedPolicy is not null,
            "violatedPolicy is not null",
            "Объект нарушенной политики должен быть указан");

        return new EnqueueResult(violatedPolicy, null);
    }
}