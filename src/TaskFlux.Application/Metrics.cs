using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime;

namespace TaskFlux.Application;

public static class Metrics
{
    public static readonly Meter TaskFluxMeter = new("TaskFlux", "1.0.0");

    public static readonly Counter<long> TotalAcceptedRequests = TaskFluxMeter.CreateCounter<long>(
        name: "taskflux.requests.accepted.total",
        unit: null,
        description: "Общее количество принятых запросов");

    public static readonly Counter<long> TotalProcessedRequests = TaskFluxMeter.CreateCounter<long>(
        name: "taskflux.requests.processed.total",
        unit: null,
        description: "Общее количество обработанных запросов");

    public static readonly Counter<long> TotalConnectedClients = TaskFluxMeter.CreateCounter<long>(
        name: "taskflux.clients.connected.total",
        unit: null,
        description: "Количество подключенных клиентов за все время");

    public static readonly Counter<long> TotalDisconnectedClients = TaskFluxMeter.CreateCounter<long>(
        name: "taskflux.clients.disconnected.total",
        unit: null,
        description: "Количество отключенных клиентов за все время");

    public static readonly Counter<long> TotalProcessedCommands = TaskFluxMeter.CreateCounter<long>(
        name: "taskflux.commands.processed.total",
        unit: null,
        description: "Общее количество обработанных команд");

    public static void RegisterCommandQueueLengthMetric(ExclusiveRequestAcceptor requestAcceptor)
    {
        TaskFluxMeter.CreateObservableGauge(name: "taskflux.command_queue.length",
            observeValue: () => requestAcceptor.QueueSize,
            unit: null,
            description: "Размер очереди команд");
    }

    public static readonly Histogram<long> RpcDuration = TaskFluxMeter.CreateHistogram<long>(
        name: "taskflux.rpc.duration",
        unit: "ms",
        description: "Длительность RCP запросов");

    public static readonly Counter<long> RpcSentBytes = TaskFluxMeter.CreateCounter<long>(
        name: "taskflux.rpc.sent_bytes.total",
        unit: "bytes",
        description: "Общее количество отправленных байт на другие узлы кластера");

    public static void RegisterGcMetrics()
    {
        TaskFluxMeter.CreateObservableGauge(name: "taskflux.gc.duration",
            observeValue: () => GC.GetTotalPauseDuration().TotalMilliseconds,
            unit: "ms",
            description: "Время затраченное на GC");
        TaskFluxMeter.CreateObservableGauge(name: "taskflux.gc.available.bytes",
            observeValue: () => GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
            unit: "bytes",
            description: "Доступная для выделения память в байтах");
        TaskFluxMeter.CreateObservableGauge(name: "taskflux.gc.allocations.bytes.total",
            observeValue: () => GC.GetTotalAllocatedBytes(precise: false),
            unit: "bytes",
            description: "Общее количество выделенной памяти за все время работы приложения");
        TaskFluxMeter.CreateObservableUpDownCounter(name: "taskflux.gc.committed_memory.bytes",
            observeValue: () => GC.GetGCMemoryInfo().TotalCommittedBytes,
            unit: "bytes",
            description: "Общее количество зарезервированной памяти");
        TaskFluxMeter.CreateObservableGauge(name: "taskflux.gc.objects.size.bytes",
            observeValue: () => GC.GetTotalMemory(forceFullCollection: false),
            unit: "bytes",
            description: "Место занимаемое всеми объектами в куче в байтах без учета фрагментации");

        var generations = new[] {"gen0", "gen1", "gen2", "loh", "poh"};

        IEnumerable<Measurement<long>> GetGcCollectionsCount()
        {
            var prevGenerationCollectionsCount = 0L;
            for (int currentGeneration = GC.MaxGeneration; currentGeneration >= 0; currentGeneration--)
            {
                var collectionsFromCurrentGeneration = GC.CollectionCount(currentGeneration);
                yield return new Measurement<long>(collectionsFromCurrentGeneration - prevGenerationCollectionsCount,
                    new KeyValuePair<string, object?>("generation", generations[currentGeneration]));
                prevGenerationCollectionsCount = collectionsFromCurrentGeneration;
            }
        }

        TaskFluxMeter.CreateObservableCounter(name: "taskflux.gc.collections.count",
            observeValues: GetGcCollectionsCount,
            description: "Количество сборок мусора произошедших с начала работы приложения");
    }

    public static void RegisterRuntimeMetrics()
    {
        TaskFluxMeter.CreateObservableCounter(name: "taskflux.jit.il_compiled.bytes",
            observeValue: () => JitInfo.GetCompiledILBytes(currentThread: false),
            unit: "bytes",
            description: "Количество байт IL кода, который был скомпилирован в нативный");
        TaskFluxMeter.CreateObservableCounter(name: "taskflux.jit.compilation_time",
            observeValue: () => JitInfo.GetCompilationTime().TotalMilliseconds,
            unit: "ms",
            description: "Время затраченное на JIT компиляцию");
        TaskFluxMeter.CreateObservableCounter(name: "taskflux.monitor.lock_contention.count",
            observeValue: () => Monitor.LockContentionCount,
            unit: null,
            description: "Количество Lock Contention, произошедших с момента старта приложения");
        TaskFluxMeter.CreateObservableUpDownCounter(name: "taskflux.thread_pool.threads.count",
            observeValue: () => ThreadPool.ThreadCount,
            unit: null,
            description: "Размер пула потоков");
        TaskFluxMeter.CreateObservableUpDownCounter(name: "taskflux.thread_pool.queue.length",
            observeValue: () => ThreadPool.PendingWorkItemCount,
            unit: null,
            description: "Количество ожидающих к выполнению задач в пуле потоков");

        var currentProcess = Process.GetCurrentProcess();
        TaskFluxMeter.CreateObservableUpDownCounter(name: "taskflux.thread.count",
            observeValue: () => currentProcess.Threads.Count,
            description: "Количество запущенных потоков");
        TaskFluxMeter.CreateObservableGauge(name: "taskflux.timer.count",
            observeValue: () => Timer.ActiveCount,
            description: "Количество запущенных таймеров");

        var exceptionCounter = TaskFluxMeter.CreateCounter<long>(name: "taskflux.exception.count",
            description: "Количество возникших исключений");
        AppDomain.CurrentDomain.FirstChanceException += (_, _) => exceptionCounter.Add(1);
    }
}