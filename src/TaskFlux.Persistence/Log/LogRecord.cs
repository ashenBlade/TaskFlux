namespace TaskFlux.Consensus.Persistence.Log;

internal class LogRecord
{
    public LogRecord(Term term, uint checkSum, byte[] payload, int payloadLength, long position)
    {
        Term = term;
        CheckSum = checkSum;
        Payload = payload;
        PayloadLength = payloadLength;
        Position = position;
    }

    public long GetNextRecordPosition()
    {
        return Position       // Начало в файле
             + sizeof(int)    // Маркер
             + sizeof(long)   // Терм
             + sizeof(uint)   // Чек-сумма
             + sizeof(int)    // Длина данных
             + PayloadLength; // Размер данных
    }

    public long GetDataStartPosition()
    {
        return Position      // Начало в файле самой записи 
             + sizeof(int)   // Маркер
             + sizeof(long)  // Терм
             + sizeof(uint); // Чек-сумма
    }

    public void ClearPayload()
    {
        Payload = Array.Empty<byte>();
    }

    /// <summary>
    /// Обновить содержимое поля <see cref="Payload"/>.
    /// Размер содержимого должен совпадать с размером предыдущего содержимого
    /// </summary>
    public void SetPayload(byte[] payload)
    {
        if (payload.Length != PayloadLength)
        {
            throw new InvalidOperationException(
                $"Размер выставляемого содержимого не равен указанному. Размер выставляемого: {payload.Length}. Указанный размер: {PayloadLength}");
        }

        Payload = payload;
    }

    public Term Term { get; init; }
    public uint CheckSum { get; init; }
    public byte[] Payload { get; private set; }
    public int PayloadLength { get; private set; }
    public long Position { get; init; }
}