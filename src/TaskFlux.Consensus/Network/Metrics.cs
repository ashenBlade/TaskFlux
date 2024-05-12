using System.Diagnostics.Metrics;

namespace TaskFlux.Consensus.Network;

public static class Metrics
{
    public static readonly Meter ClusterMeter = new("TaskFlux.Cluster", "1.0.0");

    public static readonly Histogram<long> RpcDuration = ClusterMeter.CreateHistogram<long>(
        name: "taskflux.rpc.duration",
        unit: "ms",
        description: "Длительность RCP запросов");

    public static readonly Counter<long> RpcSentBytes = ClusterMeter.CreateCounter<long>(
        name: "taskflux.rpc.sent_bytes.total",
        unit: "bytes",
        description: "Общее количество отправленных байт на другие узлы кластера");
}