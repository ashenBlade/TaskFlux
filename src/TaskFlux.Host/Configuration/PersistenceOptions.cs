using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Configuration;

public class PersistenceOptions
{
    public static readonly string DefaultDataDirectory = Path.Combine("/var", "lib", "tflux");

    [Required(ErrorMessage = "Рабочая директория не указана")]
    public string WorkingDirectory { get; set; } = DefaultDataDirectory;

    public const long DefaultLogFileSoftLimit = 1024 * 1024 * 16; /* 16 Мб */

    [Range(16, long.MaxValue, ErrorMessage = "Размер сегмента лога должен быть больше 16 байт")]
    public long LogFileSoftLimit { get; set; } = DefaultLogFileSoftLimit;

    public const long DefaultLogFileHardLimit = 1024 * 1024 * 32; /* 32 Мб */

    [Range(16, long.MaxValue, ErrorMessage = "Размер сегмента лога должен быть больше 16 байт")]
    public long LogFileHardLimit { get; set; } = DefaultLogFileHardLimit;

    public const int DefaultSnapshotCreationSegmentsThreshold = 5;
    public int SnapshotCreationSegmentsThreshold { get; set; } = DefaultSnapshotCreationSegmentsThreshold;

    public const long DefaultReplicationMaxSendSize = 1024 * 1024; /* 1 Мб */

    [Range(0, long.MaxValue, ErrorMessage = "Максимальный размер отправляемых записей должен быть положительным")]
    public long ReplicationMaxSendSize { get; set; }

    public static PersistenceOptions FromConfiguration(IConfiguration configuration)
    {
        return new PersistenceOptions()
        {
            WorkingDirectory = configuration.GetValue(nameof(WorkingDirectory), DefaultDataDirectory)!,
            LogFileSoftLimit = configuration.GetValue(nameof(LogFileSoftLimit), DefaultLogFileSoftLimit),
            LogFileHardLimit = configuration.GetValue(nameof(LogFileHardLimit), DefaultLogFileHardLimit),
            SnapshotCreationSegmentsThreshold = configuration.GetValue(nameof(SnapshotCreationSegmentsThreshold),
                DefaultSnapshotCreationSegmentsThreshold),
            ReplicationMaxSendSize =
                configuration.GetValue(nameof(ReplicationMaxSendSize), DefaultReplicationMaxSendSize),
        };
    }
}