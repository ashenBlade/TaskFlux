using TaskFlux.Core;

namespace TaskFlux.Commands;

/// <summary>
/// Команда, предназначенная только для чтения - не модифицирует данные
/// </summary>
public abstract class ReadOnlyCommand : Command
{
    public sealed override bool IsReadOnly => true;

    public sealed override Result Apply(IApplication application)
    {
        return Apply(application);
    }

    public sealed override void ApplyNoResult(IApplication context)
    {
        ApplyNoResult(context);
    }

    protected abstract Result Apply(IReadOnlyApplication context);
    protected abstract void ApplyNoResult(IReadOnlyApplication context);
}