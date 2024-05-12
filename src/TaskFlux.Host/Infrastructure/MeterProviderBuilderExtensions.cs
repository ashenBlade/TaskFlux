using OpenTelemetry.Metrics;
using TaskFlux.Application;
using TaskFlux.Persistence;
using Metrics = TaskFlux.Application.Metrics;

namespace TaskFlux.Host.Infrastructure;

public static class MeterProviderBuilderExtensions
{
    public static void AddTaskFluxMetrics(this MeterProviderBuilder metrics,
        FileSystemPersistenceFacade persistence,
        ExclusiveRequestAcceptor requestAcceptor)
    {
        Metrics.RegisterGcMetrics();
        Metrics.RegisterRuntimeMetrics();
        Metrics.RegisterCommandQueueLengthMetric(requestAcceptor);
        TaskFlux.Persistence.Metrics.RegisterPersistenceMetrics(persistence);
        metrics.AddMeter(
                Persistence.Metrics.PersistenceMeter.Name,
                Consensus.Network.Metrics.ClusterMeter.Name,
                Metrics.TaskFluxMeter.Name)
            .AddPrometheusExporter();
    }
}