namespace Raft.StateMachine.JobQueue.StringSerialization.Responses;

public abstract class BaseStringResponse: IResponse
{
    public void WriteTo(Stream output)
    {
        using var writer = new StreamWriter(output);
        Accept(writer);
        writer.Flush();
    }

    protected abstract void Accept(StreamWriter writer);
}