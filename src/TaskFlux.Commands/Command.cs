using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands;

/// <summary>
/// Базовый класс команды, которая может быть выполнена над узлом
/// </summary>
public abstract class Command
{
    protected internal Command()
    {
    }

    public virtual bool IsReadOnly => true;
    public abstract CommandType Type { get; }

    public abstract Result Apply(ICommandContext context);
    public abstract void ApplyNoResult(ICommandContext context);

    public abstract void Accept(ICommandVisitor visitor);
    public abstract T Accept<T>(IReturningCommandVisitor<T> visitor);
    public abstract ValueTask AcceptAsync(IAsyncCommandVisitor visitor, CancellationToken token = default);
}