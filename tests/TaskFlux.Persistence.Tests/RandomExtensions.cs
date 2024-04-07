namespace TaskFlux.Persistence.Tests;

public static class RandomExtensions
{
    public static byte[] ByteArray(this Random random, int length)
    {
        var array = new byte[length];
        random.NextBytes(array);
        return array;
    }
}