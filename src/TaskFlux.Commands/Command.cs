using TaskFlux.Commands.Visitors;
using TaskFlux.Core;
using TaskFlux.Serialization;

namespace TaskFlux.Commands;

/// <summary>
/// Базовый класс команды, которая может быть выполнена над узлом.
/// От нее настледуются 2 типа команд: модифицирующие и только для чтения.
/// </summary>
public abstract class Command
{
    protected internal Command()
    {
    }

    public abstract bool IsReadOnly { get; }
    public abstract CommandType Type { get; }

    public abstract Response Apply(IApplication application);
    public abstract void ApplyNoResult(IApplication context);

    public abstract bool TryGetDelta(out Delta delta);


    public abstract void Accept(ICommandVisitor visitor);
    public abstract T Accept<T>(ICommandVisitor<T> visitor);
}