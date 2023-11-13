using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

namespace TaskFlux.Commands.Serialization.Tests;

// ReSharper disable UnusedParameter.Local
public class CommandEqualityComparer : IEqualityComparer<Command>
{
    public static readonly CommandEqualityComparer Instance = new();

    public bool Equals(Command? x, Command? y)
    {
        return Check(( dynamic ) x!, ( dynamic ) y!);
    }

    private bool Check(CountCommand left, CountCommand right) => true;

    private bool Check(EnqueueCommand left, EnqueueCommand right) =>
        left.Key == right.Key && left.Payload.SequenceEqual(right.Payload);

    private bool Check(DequeueCommand left, DequeueCommand right) => true;

    private bool Check(CreateQueueCommand left, CreateQueueCommand right) =>
        left.Name == right.Name
     && left.MaxQueueSize == right.MaxQueueSize
     && left.MaxPayloadSize == right.MaxPayloadSize
     && left.PriorityRange == right.PriorityRange;

    public int GetHashCode(Command obj)
    {
        return ( int ) obj.Type;
    }
}