namespace TaskFlux.PriorityQueue.Tests;

public static class RandomExtensions
{
    public static byte[] RandomBuffer(this Random random, int? size = null)
    {
        var buffer = new byte[size ?? random.Next(0, 100)];
        random.NextBytes(buffer);
        return buffer;
    }
}