using TaskFlux.Core.Commands.Visitors;

namespace TaskFlux.Core.Commands;

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
}