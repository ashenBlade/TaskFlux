namespace Consensus.Raft.State.LeaderState;

public class HeartbeatSynchronizer : IDisposable
{
    private readonly ManualResetEvent _waitHandle = new(false);
    private volatile bool _end;
    private Term? _greaterTerm;

    public void NotifySuccess()
    {
        if (_end)
        {
            return;
        }

        try
        {
            _waitHandle.Set();
            _end = true;
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public void NotifyFoundGreaterTerm(Term greaterTerm)
    {
        if (_end)
        {
            return;
        }

        _greaterTerm = greaterTerm;
        try
        {
            _waitHandle.Set();
            _end = true;
        }
        catch (ObjectDisposedException)
        {
        }
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
        _end = true;
        _waitHandle.Dispose();
    }
}