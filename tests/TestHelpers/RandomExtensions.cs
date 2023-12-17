namespace TestHelpers;

public static class RandomExtensions
{
    public static byte[] RandomBytes(this Random random, int? count = null)
    {
        var c = count ?? random.Next(0, 1025);
        var buffer = new byte[c];
        random.NextBytes(buffer);
        return buffer;
    }
}