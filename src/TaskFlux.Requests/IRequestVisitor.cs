using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Requests.JobQueue.Enqueue;

namespace TaskFlux.Requests;

public interface IRequestVisitor
{
    public void Visit(GetCountRequest getCountRequest);
    public void Visit(EnqueueRequest enqueueRequest);
    public void Visit(DequeueRequest dequeueRequest);
    
    public void Visit(BatchRequest batchRequest);
}