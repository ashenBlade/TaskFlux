using TaskFlux.Commands.Visitors;
using TaskFlux.Core;

namespace TaskFlux.Commands;

/// <summary>
/// Базовый класс команды, которая может быть выполнена над узлом.
/// От нее наследуются 2 типа команд: модифицирующие и только для чтения.
/// </summary>
public abstract class Command
{
    protected internal Command()
    {
    }

    public abstract Response Apply(IApplication application);

    public abstract void Accept(ICommandVisitor visitor);
    public abstract T Accept<T>(ICommandVisitor<T> visitor);
}