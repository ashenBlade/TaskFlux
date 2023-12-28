using System.Collections.Concurrent;
using Consensus.Raft;

namespace Consensus.JobQueue;

public class ThreadPerWorkerBackgroundJobQueue : IBackgroundJobQueue, IDisposable
{
    private readonly CancellationTokenSource _cts = new();

    private readonly (Thread Thread, BlockingCollection<(IBackgroundJob Job, CancellationToken Token)> Queue)?[]
        _workers;

    public ThreadPerWorkerBackgroundJobQueue(int clusterSize, int currentNodeId)
    {
        _workers = CreateWorkers(clusterSize, currentNodeId);
    }

    private (Thread, BlockingCollection<(IBackgroundJob Job, CancellationToken Token)>)?[] CreateWorkers(
        int clusterSize,
        int currentNodeId)
    {
        var array = new (Thread, BlockingCollection<(IBackgroundJob Job, CancellationToken Token)>)?[clusterSize];
        for (var i = 0; i < array.Length; i++)
        {
            if (i == currentNodeId)
            {
                continue;
            }

            var thread = new Thread(ThreadWorker);
            var queue = new BlockingCollection<(IBackgroundJob Job, CancellationToken Token)>();
            array[i] = ( thread, queue );
        }

        return array;
    }

    public void Start()
    {
        for (var i = 0; i < _workers.Length; i++)
        {
            if (_workers[i] is var (thread, _))
            {
                thread.Start();
            }
        }
    }

    public void Accept(IBackgroundJob job, CancellationToken token)
    {
        var index = job.NodeId.Id;
        // На 0 можно не проверять - ID узла отрицательным быть не может
        if (_workers.Length <= index)
        {
            throw new ArgumentOutOfRangeException(nameof(job.NodeId), job.NodeId,
                "Указанный ID узла выходит за рамки кластера");
        }

        if (_workers[index] is not var (_, collection))
        {
            throw new ArgumentOutOfRangeException(nameof(job.NodeId), job.NodeId,
                "Обработчика текущего узла не предусмотрено");
        }

        try
        {
            collection.Add(( job, token ), _cts.Token);
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private void ThreadWorker(object? o)
    {
        var index = ( int ) o!;
        var collection = _workers[index]!.Value.Queue;

        var backgroundJobQueueToken = _cts.Token;
        try
        {
            foreach (var (job, jobToken) in collection.GetConsumingEnumerable(backgroundJobQueueToken))
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(backgroundJobQueueToken, jobToken);
                try
                {
                    job.Run(cts.Token);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public void Dispose()
    {
        _cts.Cancel();

        Array.ForEach(_workers, static w =>
        {
            if (w is var (thread, collection))
            {
                collection.Dispose();
                thread.Join();
            }
        });

        _cts.Dispose();
    }
}