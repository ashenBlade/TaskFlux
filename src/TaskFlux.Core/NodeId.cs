namespace TaskFlux.Core;

public readonly struct NodeId : IEquatable<NodeId>
{
    /// <summary>
    /// Стандартный конструктор для Id узла.
    /// Принимает числовое значение.
    /// </summary>
    /// <param name="id">Числовое значение ID узла. Должен быть неотрицательным</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="id"/> - отрицательный</exception>
    public NodeId(int id)
    {
        if (id < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(id), id,
                "Id узла должно быть положительным значением");
        }

        Id = id;
    }

    public const int StartId = 0;

    public NodeId()
    {
        Id = StartId;
    }

    public static explicit operator int(NodeId id) => id.Id;

    public static NodeId Start => new(StartId);
    public int Id { get; }

    public override string ToString()
    {
        return $"NodeId({Id})";
    }

    public override bool Equals(object? obj)
    {
        return obj is NodeId nodeId
            && nodeId.Id == Id;
    }

    public bool Equals(NodeId other)
    {
        return Id == other.Id;
    }

    public static bool operator ==(NodeId left, NodeId right)
    {
        return left.Id == right.Id;
    }

    public static bool operator !=(NodeId left, NodeId right)
    {
        return left.Id != right.Id;
    }

    public override int GetHashCode()
    {
        return Id;
    }
}