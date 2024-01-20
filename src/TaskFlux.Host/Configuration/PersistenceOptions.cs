using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;

namespace TaskFlux.Host.Configuration;

public class PersistenceOptions
{
    public static readonly string DefaultDataDirectory = Path.Combine("/var", "lib", "tflux");

    [Required(ErrorMessage = "Рабочая директория не указана")]
    public string WorkingDirectory { get; set; } = DefaultDataDirectory;

    public const ulong DefaultMaxLogFileSize = 1024 * 1024; /* 1 Мб */

    [Range(16, int.MaxValue, ErrorMessage = "Размер файла лога должен быть больше 16 байт")]
    public ulong MaxLogFileSize { get; set; } = DefaultMaxLogFileSize;

    public static PersistenceOptions FromConfiguration(IConfiguration configuration)
    {
        return new PersistenceOptions()
        {
            WorkingDirectory = configuration.GetValue(nameof(WorkingDirectory), DefaultDataDirectory)!,
            MaxLogFileSize = configuration.GetValue(nameof(MaxLogFileSize), DefaultMaxLogFileSize),
        };
    }
}