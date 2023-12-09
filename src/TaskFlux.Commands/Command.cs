using TaskFlux.Commands.Visitors;
using TaskFlux.Core;

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

    /// <summary>
    /// Должен ли быть выполнен быстрый путь выполнения - без репликации
    /// </summary>
    /// <remarks>
    /// Этот параметр влияет на порядок выполнения команды внутри пайплайна.
    /// Если эта команда должна выполнять модификации, но при этом выставлен этот флаг,
    /// то состояние будет изменено, но это не будет зафиксировано.
    /// </remarks>
    public abstract bool UseFastPath { get; }

    public abstract Response Apply(IApplication application);

    public abstract void Accept(ICommandVisitor visitor);
    public abstract T Accept<T>(ICommandVisitor<T> visitor);
}