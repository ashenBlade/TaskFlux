using TaskFlux.Core;

namespace TaskFlux.Commands;

/// <summary>
/// Команда, предназначенная только для чтения - не модифицирует данные
/// </summary>
public abstract class ReadOnlyCommand : Command
{
    public sealed override Response Apply(IApplication application)
    {
        return Apply(application);
    }

    protected abstract Response Apply(IReadOnlyApplication context);
    protected abstract void ApplyNoResult(IReadOnlyApplication context);
}