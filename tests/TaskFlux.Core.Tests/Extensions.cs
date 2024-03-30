namespace TaskFlux.Core.Tests;

public static class Extensions
{
    public static T Apply<T>(this T value, Action<T> apply)
    {
        apply(value);
        return value;
    }
}