using TaskFlux.Application.Persistence.Delta;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Persistence;

internal static class MemoryBinaryWriterExtensions
{
    public static void Write(this ref MemoryBinaryWriter writer, DeltaType type)
    {
        writer.Write(( byte ) type);
    }
}