using TaskFlux.Utils.CheckSum;

namespace TaskFlux.Consensus.Persistence;

public record LogEntry(Term Term, byte[] Data)
{
    private uint? _checkSum;
    public uint GetCheckSum() => _checkSum ??= Crc32CheckSum.Compute(Data);
}