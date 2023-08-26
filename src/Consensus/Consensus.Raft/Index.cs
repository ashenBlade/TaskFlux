namespace Consensus.Raft;

/// <summary>
/// Структура, представляющая индекс команды в файле лога 
/// </summary>
/// <param name="Value">Сырое значение индекса</param>
public readonly record struct Index(uint Value)
{
    public const uint DefaultIndex = 0;
    public static readonly Index Default = new(DefaultIndex);

    public Index() : this(DefaultIndex)
    {
    }

    public static implicit operator uint(Index index) => index.Value;

    public override string ToString()
    {
        return $"Index({Value})";
    }

    public override int GetHashCode()
    {
        return Value.GetHashCode();
    }
}