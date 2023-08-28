namespace Consensus.Raft.Persistence.LogFileCheckStrategy;

public class SizeLogFileSizeChecker : ILogFileSizeChecker
{
    /// <summary>
    /// Проверка размера лога с максимальным размером по умолчанию - <see cref="Constants.Marker"/>
    /// </summary>
    public static readonly SizeLogFileSizeChecker MaxLogFileSize = new(Constants.MaxLogFileSize);

    private readonly ulong _maxSize;

    public SizeLogFileSizeChecker(ulong maxSize)
    {
        _maxSize = maxSize;
    }

    public bool IsLogFileSizeExceeded(ulong logFileSize)
    {
        // Используется строгое неравенство для оптимизации -
        // Если будет достигнут строгий предел, то после этой команды 
        // может быть много RO, поэтому делать снапшот не нужно
        return _maxSize < logFileSize;
    }
}