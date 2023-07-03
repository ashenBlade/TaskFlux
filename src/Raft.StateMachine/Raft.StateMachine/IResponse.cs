namespace Raft.StateMachine;

public interface IResponse
{
    void WriteTo(Stream output);
}