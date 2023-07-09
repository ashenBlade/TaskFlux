using System.Text;

namespace Raft.StateMachine.JobQueue;

public abstract class JobQueueResponse: IResponse
{
    protected JobQueueResponse(int marker)
    {
        Marker = marker;
    }

    public int Marker { get; }
    protected abstract void WriteTo(BinaryWriter writer);
    public void WriteTo(Stream output)
    {
        using var writer = new BinaryWriter(output, Encoding.UTF8, leaveOpen: true);
        writer.Write(Marker);
        WriteTo(writer);
        writer.Flush();
    }
}