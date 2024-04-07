using TaskFlux.Core.Queue;
using TaskFlux.Persistence.ApplicationState.Deltas;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Persistence.ApplicationState;

internal static class MemoryBinaryWriterExtensions
{
    public static void Write(this ref MemoryBinaryWriter writer, DeltaType type)
    {
        writer.Write(( byte ) type);
    }

    public static void Write(this ref MemoryBinaryWriter writer, RecordId id)
    {
        writer.Write(id.Id);
    }
}