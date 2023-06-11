namespace Raft.Core;

public readonly record struct PeerId(int Value)
{
    public static explicit operator int(PeerId id) => id.Value;

    public override string ToString()
    {
        return $"PeerId({Value})";
    }
}