namespace Raft.StateMachine.JobQueue.Tests;

public static class ArrayExtensions
{
    public static IEnumerable<T> Cycle<T>(this T[] collection, int count)
    {
        if (collection.Length == 0)
        {
            yield break;
        }

        for (int i = 0, j = 0; i < count; i++, j = j == collection.Length - 1
                                                       ? 0
                                                       : j + 1)
        {
            yield return collection[j];
        }
    }
}