namespace Raft.Core;

public readonly record struct PeerId(int Value)
{
    public static explicit operator int(PeerId id) => id.Value;

    public static PeerId operator +(PeerId id, int inc) => new(id.Value + inc);
    public static PeerId None => new(0);

    public override string ToString()
    {
        return $"PeerId({Value})";
    }
}