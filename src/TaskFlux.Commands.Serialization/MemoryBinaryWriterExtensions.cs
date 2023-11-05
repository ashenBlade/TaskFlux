using Utils.Serialization;

namespace TaskFlux.Commands.Serialization;

internal static class MemoryBinaryWriterExtensions
{
    public static void Write(this ref MemoryBinaryWriter writer, ResponseType responseType)
    {
        writer.Write(( byte ) responseType);
    }

    public static void Write(this ref MemoryBinaryWriter writer, PolicyCode policyCode)
    {
        writer.Write(( int ) policyCode);
    }
}