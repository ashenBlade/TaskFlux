namespace TaskFlux.Core;

public readonly struct NodeId : IEquatable<NodeId>
{
    /// <summary>
    /// Стандартный конструктор для Id узла.
    /// Принимает числовое значение.
    /// </summary>
    /// <param name="value">Числовое значение ID узла. Должен быть положительным</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> - не положительный</exception>
    public NodeId(int value)
    {
        if (value < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Id узла должно быть положительным значением");
        }

        Value = value;
    }

    public const int StartId = 1;

    public NodeId()
    {
        Value = StartId;
    }

    public static explicit operator int(NodeId id) => id.Value;

    public static NodeId Start => new(StartId);
    public int Value { get; }

    public override string ToString()
    {
        return $"NodeId({Value})";
    }

    public override bool Equals(object? obj)
    {
        return obj is NodeId nodeId
            && nodeId.Value == Value;
    }

    public bool Equals(NodeId other)
    {
        return Value == other.Value;
    }

    public static bool operator ==(NodeId left, NodeId right)
    {
        return left.Value == right.Value;
    }

    public static bool operator !=(NodeId left, NodeId right)
    {
        return left.Value != right.Value;
    }

    public override int GetHashCode()
    {
        return Value;
    }
}