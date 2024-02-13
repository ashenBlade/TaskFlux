namespace TaskFlux.Host.Infrastructure;

public class HostApplicationLifetime : Application.IApplicationLifetime
{
    private readonly TaskCompletionSource<ExitCode> _lifetime = new();
    private readonly CancellationTokenSource _cts = new();

    private enum ExitCode
    {
        Normal = 0,
        Abnormal = 1,
    }

    public void Stop()
    {
        _lifetime.TrySetResult(ExitCode.Normal);
        _cts.Cancel();
    }

    public void StopAbnormal()
    {
        _lifetime.TrySetResult(ExitCode.Abnormal);
        _cts.Cancel();
    }

    public CancellationToken Token => _cts.Token;

    public async Task<int> WaitReturnCode()
    {
        return ( int ) await _lifetime.Task;
    }
}