using TaskFlux.Commands.Visitors;

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
    public abstract ValueTask AcceptAsync(IAsyncResponseVisitor visitor, CancellationToken token = default);
}