namespace TestHelpers;

public class LambdaEqualityComparer<T> : IEqualityComparer<T>
{
    private readonly Func<T?, T?, bool> _equals;
    private readonly Func<T, int> _hashCode;

    public LambdaEqualityComparer(Func<T?, T?, bool> equals, Func<T, int> hashCode)
    {
        _equals = equals;
        _hashCode = hashCode;
    }

    public bool Equals(T? x, T? y)
    {
        return _equals(x, y);
    }

    public int GetHashCode(T obj)
    {
        return _hashCode(obj);
    }
}