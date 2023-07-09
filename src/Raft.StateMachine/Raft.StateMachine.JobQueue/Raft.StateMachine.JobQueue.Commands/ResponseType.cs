namespace Raft.StateMachine.JobQueue.Commands;

public enum ResponseType
{
    Dequeue = 1,
    Enqueue = 2,
    GetCount = 3,
    Error = 4,
}