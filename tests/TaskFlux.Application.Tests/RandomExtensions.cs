namespace TaskFlux.Application.Tests;

public static class RandomExtensions
{
    public static byte[] RandomBuffer(this Random random, int? length = null)
    {
        var len = length ?? random.Next(0, 128);
        var buffer = new byte[len];
        random.NextBytes(buffer);
        return buffer;
    }
}