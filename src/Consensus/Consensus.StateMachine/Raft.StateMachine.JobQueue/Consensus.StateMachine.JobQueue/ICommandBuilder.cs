using TaskFlux.Requests;

namespace Consensus.StateMachine.JobQueue;

public interface ICommandBuilder
{
    public ICommand BuildCommand(IRequest request);
}