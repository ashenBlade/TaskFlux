using Utils.Serialization;

namespace TaskFlux.Delta;

internal static class MemoryBinaryWriterExtensions
{
    public static void Write(this ref MemoryBinaryWriter writer, DeltaType type)
    {
        writer.Write(( byte ) type);
    }
}