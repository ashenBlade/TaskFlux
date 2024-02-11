using TaskFlux.Consensus;

namespace TaskFlux.Persistence.Log;

public class SegmentedFileLogOptions
{
    public const long DefaultSoftLimit = 1024 * 1024 * 16; // 64 Мб
    public const long DefaultHardLimit = 1024 * 1024 * 32; // 128 Мб

    public static SegmentedFileLogOptions Default => new();

    public SegmentedFileLogOptions(long softLimit, long hardLimit, bool preallocateSegment, long maxReadEntriesSize)
    {
        if (hardLimit < softLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(hardLimit), "hardLimit",
                "Жесткий предел размера файла лога не может быть меньше мягкого предела");
        }

        if (softLimit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(softLimit), softLimit,
                "Мягкий предел не может быть отрицательным");
        }

        if (hardLimit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(hardLimit), hardLimit,
                "Жесткий предел не может быть отрицательным");
        }

        if (maxReadEntriesSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxReadEntriesSize), maxReadEntriesSize,
                "Максимальный размер прочитанных записей из лога не может быть отрицательным");
        }

        LogFileSoftLimit = softLimit;
        LogFileHardLimit = hardLimit;
        PreallocateSegment = preallocateSegment;
        MaxReadEntriesSize = maxReadEntriesSize;
    }

    public SegmentedFileLogOptions()
    {
        LogFileSoftLimit = DefaultSoftLimit;
        LogFileHardLimit = DefaultHardLimit;
        PreallocateSegment = true;
    }

    /// <summary>
    /// Нужно ли выделять место под создаваемый файл сегмента при его создании.
    /// Выделяется размер указанный в поле <see cref="LogFileSoftLimit"/>.
    /// Если указан <c>true</c>, то при создании будет выделено ровно столько, сколько указано, включая рабочие заголовки и поля.
    /// В противном случае, место будет выделяться при каждой записи.
    /// </summary>
    public bool PreallocateSegment { get; }

    /// <summary>
    /// Мягкий предел размера файла сегмента лога
    /// </summary>
    public long LogFileSoftLimit { get; }


    /// <summary>
    /// Жесткий предел размера сегмента лога
    /// </summary>
    public long LogFileHardLimit { get; }

    /// <summary>
    /// Максимальный размер данных, прочитанных из лога за раз при использовании метода <see cref="IPersistence.TryGetFrom"/>.
    /// Используется чтобы не выделять слишком большой участок памяти за раз.
    /// </summary>
    public long MaxReadEntriesSize { get; }
}