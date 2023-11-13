using TaskFlux.Models;

namespace Consensus.Raft.Tests.Infrastructure;

public static class Extensions
{
    public static T Apply<T>(this T obj, Action<T> apply)
    {
        apply(obj);
        return obj;
    }

    public static NodeId? ToNodeId(this int? nodeId) => nodeId is { } ni
                                                            ? new NodeId(ni)
                                                            : null;

    public static (T[] Left, T[] Right) Split<T>(this IReadOnlyList<T> array, int index)
    {
        var leftLength = index + 1;
        var left = new T[leftLength];
        var rightLength = array.Count - index - 1;

        var right = new T[rightLength];
        for (int i = 0; i <= index; i++)
        {
            left[i] = array[i];
        }

        for (int i = index + 1, j = 0; i < array.Count; i++, j++)
        {
            right[j] = array[i];
        }

        return ( left, right );
    }

    public static T[] ToArray<T>(this IEnumerable<T> array, int hint = -1)
    {
        if (hint == -1)
        {
            return array.ToArray();
        }

        var result = new T[hint];
        var i = 0;
        foreach (var item in array)
        {
            result[i] = item;
            i++;
        }

        return result;
    }
}