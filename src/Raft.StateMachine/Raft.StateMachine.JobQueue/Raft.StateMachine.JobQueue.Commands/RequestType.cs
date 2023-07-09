namespace Raft.StateMachine.JobQueue.Commands;

public enum RequestType
{
    EnqueueRequest = 1,
    DequeueRequest = 2,
    GetCountRequest = 3
}