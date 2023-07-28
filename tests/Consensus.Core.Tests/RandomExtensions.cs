namespace Consensus.Core.Tests;

public static class RandomExtensions
{
    public static T[] Shuffle<T>(this Random random, T[] array)
    {
        for (var i = 0; i < array.Length; i++)
        {
            var j = i + random.Next(0, array.Length - i);
            ( array[j], array[i] ) = ( array[i], array[j] );
        }

        return array;
    }
}