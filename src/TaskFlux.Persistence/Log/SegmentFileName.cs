using System.Diagnostics;

namespace TaskFlux.Consensus.Persistence.Log;

public readonly struct SegmentFileName(Lsn startLsn) : IEquatable<SegmentFileName>
{
    /// <summary>
    /// Длина части, указывающая на стартовый индекс записи
    /// </summary>
    private const int LsnNumberLength = 19;

    private const string LsnNumberLengthStringFormat = "D19";

    /// <summary>
    /// Общая длина названия файла сегмента
    /// </summary>
    private const int FileNameTotalLength = LsnNumberLength + 4;

    public Lsn StartLsn { get; } = startLsn;

    public SegmentFileName() : this(0)
    {
    }

    public string GetFileName()
    {
        return string.Create(FileNameTotalLength, StartLsn, (span, lsn) =>
        {
            var success = lsn.Value.TryFormat(span, out var written, LsnNumberLengthStringFormat);
            Debug.Assert(success, "success", "Форматирование названия файла должно закончиться успешно");
            Debug.Assert(written == LsnNumberLength, "written == LsnNumberLength",
                "Записанное кол-во символов должно быть равно 19");
            ".log".CopyTo(span[LsnNumberLength..]);
        });
    }

    public static bool TryParse(ReadOnlySpan<char> name, out SegmentFileName fileName)
    {
        if (name.Length != FileNameTotalLength)
        {
            fileName = default;
            return false;
        }

        if (long.TryParse(name[..LsnNumberLength], out var result) && ".log" == name[LsnNumberLength..])
        {
            try
            {
                fileName = new SegmentFileName(result);
            }
            catch (ArgumentOutOfRangeException)
            {
                fileName = default;
                return false;
            }

            return true;
        }

        fileName = default!;
        return false;
    }


    public bool Equals(SegmentFileName other)
    {
        return StartLsn == other.StartLsn;
    }

    public override bool Equals(object? obj)
    {
        return obj is SegmentFileName other && Equals(other);
    }

    public override int GetHashCode()
    {
        return StartLsn.GetHashCode();
    }

    public override string ToString()
    {
        return GetFileName();
    }
}