namespace Consensus.Raft.Tests.Infrastructure;

public static class Extensions
{
    public static T Apply<T>(this T obj, Action<T> apply)
    {
        apply(obj);
        return obj;
    }
}