namespace TaskFlux.Consensus.Persistence.Log;

public class SegmentedFileLogOptions
{
    public const long DefaultSoftLimit = 1024 * 1024 * 64;  // 64 Мб
    public const long DefaultHardLimit = 1024 * 1024 * 128; // 128 Мб

    public static SegmentedFileLogOptions Default => new();

    public SegmentedFileLogOptions(long softLimit, long hardLimit)
    {
        if (hardLimit < softLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(hardLimit), "hardLimit",
                "Жесткий предел размера файла лога не может быть меньше мягкого предела");
        }

        SegmentFileSoftLimit = softLimit;
        SegmentFileHardLimit = hardLimit;
    }

    public SegmentedFileLogOptions()
    {
        SegmentFileSoftLimit = DefaultSoftLimit;
        SegmentFileHardLimit = DefaultHardLimit;
    }


    /// <summary>
    /// Мягкий предел размера файла сегмента лога
    /// </summary>
    public long SegmentFileSoftLimit { get; }


    /// <summary>
    /// Жесткий предел размера сегмента лога
    /// </summary>
    public long SegmentFileHardLimit { get; }
}