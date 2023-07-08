using System.Text;

namespace Raft.Core.Log;

/// <summary>
/// Результат применения коммита к логу
/// </summary>
/// <param name="Previous">Индекс предыдущего коммита</param>
/// <param name="Current">Индекс нового коммита</param>
public readonly record struct CommitDelta(int Previous, int Current)
{
    public bool HasChanges => Previous < Current;
    public bool SingleEntryCommit => Current - Previous == 1;
}