using System.Diagnostics.Metrics;

namespace TaskFlux.Persistence;

public static class Metrics
{
    public static readonly Meter PersistenceMeter = new Meter("TaskFlux.Persistence", "1.0.0");

    public static readonly Histogram<long> FsyncDuration = PersistenceMeter.CreateHistogram<long>(
        name: "taskflux.fs.fsync.duration",
        unit: "ms",
        description: "Время выполнения fsync()");

    public static void RegisterPersistenceMetrics(FileSystemPersistenceFacade persistence)
    {
        PersistenceMeter.CreateObservableUpDownCounter(name: "taskflux.raft.log.segments.count",
            observeValue: () => persistence.Log.GetSegmentsCount(),
            description: "Количество сегментов лога");
        PersistenceMeter.CreateObservableUpDownCounter(name: "taskflux.raft.log.segments.size",
            observeValue: () => persistence.Log.CalculateLogSegmentsTotalSize(),
            unit: "bytes",
            description: "Суммарный размер всех файлов лога в байтах");

        PersistenceMeter.CreateObservableCounter(name: "taskflux.raft.term",
            observeValue: () => persistence.CurrentTerm.Value,
            unit: null,
            description: "Текущий терм узла");
        PersistenceMeter.CreateObservableCounter(name: "taskflux.raft.snapshot.index",
            observeValue: () => persistence.Snapshot.LastApplied.Index.Value,
            unit: null,
            description: "Индекс команды в снапшоте");
        PersistenceMeter.CreateObservableUpDownCounter(name: "taskflux.raft.snapshot.size",
            observeValue: () => persistence.Snapshot.CalculateSnapshotFileSize(),
            unit: "bytes",
            description: "Размер файла снапшота в байтах");
        PersistenceMeter.CreateObservableUpDownCounter(name: "taskflux.raft.log.last_index",
            observeValue: () => persistence.LastEntry.Index.Value,
            unit: null,
            description: "Индекс последней записи в логе");
        PersistenceMeter.CreateObservableCounter(name: "taskflux.raft.log.commit_index",
            observeValue: () => persistence.CommitIndex.Value,
            unit: null,
            description: "Индекс закоммиченной команды");
        PersistenceMeter.CreateObservableCounter(name: "taskflux.raft.log.start_index",
            observeValue: () => persistence.Log.StartIndex.Value,
            description: "Индекс, с которого начинается лог");
    }
}