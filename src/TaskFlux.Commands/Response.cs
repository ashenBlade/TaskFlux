using TaskFlux.Commands.Visitors;
using TaskFlux.Serialization;

namespace TaskFlux.Commands;

/// <summary>
/// Объект результата выполнения комады
/// </summary>
public abstract class Response
{
    /// <summary>
    /// Код ответа для определения настоящего результата
    /// </summary>
    public abstract ResponseType Type { get; }

    public abstract void Accept(IResponseVisitor visitor);
    public abstract T Accept<T>(IResponseVisitor<T> visitor);

    /// <summary>
    /// Попытаться получить дельту из ответа
    /// </summary>
    /// <param name="delta">Полученная дельта изменений</param>
    /// <returns><c>true</c> - дельта получена, <c>false</c> - дельты нет</returns>
    public virtual bool TryGetDelta(out Delta delta)
    {
        // По умолчанию, изменений нет. Если они есть, то переопределить нужно
        delta = default!;
        return false;
    }
}