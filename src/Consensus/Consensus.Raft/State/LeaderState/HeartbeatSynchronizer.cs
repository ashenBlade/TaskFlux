namespace Consensus.Raft.State.LeaderState;

public class HeartbeatSynchronizer : IDisposable
{
    private readonly ManualResetEvent _waitHandle = new(false);
    private Term? _greaterTerm = null;

    public void NotifySuccess()
    {
        _waitHandle.Set();
    }

    public void NotifyFoundGreaterTerm(Term greaterTerm)
    {
        _greaterTerm = greaterTerm;
        _waitHandle.Set();
    }

    /// <summary>
    /// Дождаться завершения обработки Heartbeat запроса
    /// </summary>
    /// <param name="greaterTerm">Найденный больший терм</param>
    /// <returns>
    /// <c>true</c> - при работе с узлом был найден терм больше (<paramref name="greaterTerm"/> указывает валидное значение нового терма),
    /// <c>false</c> - Heartbeat обработан корректно
    /// </returns>
    public bool TryWaitGreaterTerm(out Term greaterTerm)
    {
        try
        {
            _waitHandle.WaitOne();
        }
        catch (ObjectDisposedException)
        {
            greaterTerm = default!;
            return false;
        }

        if (_greaterTerm is { } foundGreaterTerm)
        {
            greaterTerm = foundGreaterTerm;
            return true;
        }

        greaterTerm = Term.Start;
        return false;
    }

    public void Dispose()
    {
        _waitHandle.Dispose();
    }
}