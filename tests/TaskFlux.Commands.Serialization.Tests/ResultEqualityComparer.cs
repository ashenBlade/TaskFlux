using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

namespace TaskFlux.Commands.Serialization.Tests;

public class ResultEqualityComparer: IEqualityComparer<Result>
{
    public static readonly ResultEqualityComparer Instance = new();
    public bool Equals(Result? x, Result? y)
    {
        return Check(( dynamic ) x!, ( dynamic ) y!);
    }

    private static bool Check(CountResult first, CountResult second) => first.Count == second.Count;
    private static bool Check(EnqueueResult first, EnqueueResult second) => first.Success == second.Success;
    private static bool Check(DequeueResult first, DequeueResult second)
    {
        var firstOk = first.TryGetResult(out var k1, out var p1);
        var secondOk = second.TryGetResult(out var k2, out var p2);
        return ( firstOk, secondOk ) switch
               {
                   (true, true)   => k1 == k2 && p1.SequenceEqual(p2),
                   (false, false) => true,
                   _              => false
               };
    }
    
    public int GetHashCode(Result obj)
    {
        return ( int ) obj.Type;
    }
}