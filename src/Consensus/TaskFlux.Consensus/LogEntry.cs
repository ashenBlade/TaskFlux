using TaskFlux.Utils.CheckSum;

namespace TaskFlux.Consensus;

public record LogEntry(Term Term, byte[] Data)
{
    private uint? _checkSum;
    public uint GetCheckSum() => _checkSum ??= Crc32CheckSum.Compute(Data);

    /// <summary>
    /// Выставить предрассчитанное значение чек-суммы.
    /// Используется, когда данные были считаны из файла повторно и чек-сумма уже есть в памяти
    /// </summary>
    /// <param name="checkSum">Новая чек-сумма</param>
    public void SetCheckSum(uint checkSum)
    {
        _checkSum = checkSum;
    }

    /// <summary>
    /// Рассчитать занимаемое этой записью место в файле
    /// </summary>
    /// <returns>Размер записи в файле в байтах</returns>
    public long CalculateFileRecordSize()
    {
        return sizeof(int)  // Маркер записи
             + sizeof(long) // Терм
             + sizeof(uint) // Чек-сумма
             + sizeof(int)  // Длина данных
             + Data.Length; // Сами данные
    }

    /// <summary>
    /// Рассчитать размер самой записи, без учета вспомогательных полей.
    /// Используется для учета размера при чтении
    /// </summary>
    /// <returns>Занимаемый записью размер в байтах</returns>
    public long CalculateRecordSize()
    {
        return sizeof(long) // Терм
             + sizeof(int)  // Размер данных
             + Data.Length; // Сами данные
    }
}