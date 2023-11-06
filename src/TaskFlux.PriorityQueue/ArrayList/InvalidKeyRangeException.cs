namespace TaskFlux.PriorityQueue.ArrayList;

public class InvalidKeyRangeException : ArgumentOutOfRangeException
{
    /// <summary>
    /// Минимальное значение ключа
    /// </summary>
    public long Min { get; }

    /// <summary>
    /// Максимальное значение ключа
    /// </summary>
    public long Max { get; }

    /// <summary>
    /// Полученное значение ключа
    /// </summary>
    public long Key { get; }

    public override string? ParamName { get; }
    public override object? ActualValue => Key;

    public override string Message =>
        $"Переданный ключ вышел за границы разрешенных ключей.\nМинимальное значение: {Min}\nМаксимальное значение: {Max}\nЗначение ключа: {Key}";

    public InvalidKeyRangeException(long max, long min, long key, string? paramName)
    {
        ParamName = paramName;
        Max = max;
        Min = min;
        Key = key;
    }
}