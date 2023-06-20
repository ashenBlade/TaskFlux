using Raft.Core.Log;

namespace Raft.Core.Node;

/// <summary>
/// Эта поебота будет содержать логику по репликации хуйни всякой
/// Типа когда начинается репликация лога другие получают инстанс этой поеботы,
/// отправляют запросы.
/// Когда успешно отправили все, то каждый ставит флаг, о том что готово
/// и AppendEntriesCommand  должна получить результат как-то через нее.
/// Че там как дальше - не ебу 
/// </summary>
internal class AppendEntriesRequestSynchronizer
{
    public IReadOnlyList<LogEntry> Entries { get; }
    
    private volatile int _votes = 0;
    private readonly IQuorumChecker _quorumChecker;
    private readonly TaskCompletionSource _tcs = new();
    
    public AppendEntriesRequestSynchronizer(
        IQuorumChecker quorumChecker, 
        IReadOnlyList<LogEntry> entries)
    {
        _quorumChecker = quorumChecker;
        Entries = entries;
    }

    public Task LogReplicated => _tcs.Task;

    public void NotifyComplete()
    {
        var incremented = Interlocked.Increment(ref _votes);
        
        if (Check(incremented) && 
            !Check(incremented - 1))
        {
            try
            {
                _tcs.SetResult();
            }
            catch (InvalidOperationException)
            { }
        }

        bool Check(int count) => _quorumChecker.IsQuorumReached(count);
    }
}