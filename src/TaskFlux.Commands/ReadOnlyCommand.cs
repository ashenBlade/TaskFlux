using TaskFlux.Core;
using TaskFlux.Serialization;

namespace TaskFlux.Commands;

/// <summary>
/// Команда, предназначенная только для чтения - не модифицирует данные
/// </summary>
public abstract class ReadOnlyCommand : Command
{
    public sealed override bool IsReadOnly => true;

    public sealed override Response Apply(IApplication application)
    {
        return Apply(application);
    }

    public sealed override void ApplyNoResult(IApplication context)
    {
        ApplyNoResult(context);
    }

    public override bool TryGetDelta(out Delta delta)
    {
        delta = default!;
        return false;
    }

    protected abstract Response Apply(IReadOnlyApplication context);
    protected abstract void ApplyNoResult(IReadOnlyApplication context);
}