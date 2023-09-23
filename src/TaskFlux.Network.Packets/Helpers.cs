namespace TaskFlux.Network.Packets;

internal static class Helpers
{
    public static string CheckReturn(string? value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return value;
    }
}