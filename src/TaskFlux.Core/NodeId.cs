namespace TaskFlux.Core;

public readonly record struct NodeId(int Value)
{
    public static explicit operator int(NodeId id) => id.Value;

    public static NodeId operator +(NodeId id, int inc) => new(id.Value + inc);
    public static NodeId None => new(0);

    public override string ToString()
    {
        return $"PeerId({Value})";
    }
}