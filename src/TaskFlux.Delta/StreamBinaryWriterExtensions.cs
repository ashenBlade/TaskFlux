using Utils.Serialization;

namespace TaskFlux.Delta;

internal static class StreamBinaryWriterExtensions
{
    public static void Write(this ref StreamBinaryWriter writer, DeltaType type)
    {
        writer.Write(( byte ) type);
    }
}