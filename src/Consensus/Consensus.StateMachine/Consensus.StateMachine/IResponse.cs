namespace Consensus.StateMachine;

public interface IResponse
{
    void WriteTo(Stream output);
}