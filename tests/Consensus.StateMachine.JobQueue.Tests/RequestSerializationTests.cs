using System.Security.Cryptography;
using Consensus.StateMachine.JobQueue.Commands;
using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.GetCount;
using Consensus.StateMachine.JobQueue.Serialization;

namespace Consensus.StateMachine.JobQueue.Tests;

public class RequestSerializationTests
{
    public static readonly JobQueueRequestSerializer Serializer = new();
    public static readonly JobQueueRequestDeserializer Deserializer = new();

    private static void AssertBase(IJobQueueRequest expected)
    {
        using var memory = new MemoryStream();
        using var writer = new BinaryWriter(memory);
        
        Serializer.Serialize(expected, writer);
        var actual = Deserializer.Deserialize(memory.ToArray());
        
        Assert.Equal(expected, actual, JobQueueRequestEqualityComparer.Instance);
    }
    
    [Fact(DisplayName = nameof(DequeueRequest))]
    public void DequeueRequest__Serialization()
    {
        AssertBase(new DequeueRequest());
    }

    [Fact(DisplayName = nameof(GetCountRequest))]
    public void GetCountRequest__Serialization()
    {
        AssertBase(new GetCountRequest());
    }

    [Theory(DisplayName = nameof(EnqueueRequest))]
    [InlineData(1, new byte[] {1, 3, 4})]
    [InlineData(1, new byte[] {0})]
    [InlineData(-1, new byte[] { 0, 54, 44 })]
    [InlineData(23, new byte[] { 0, 204, 44 })]
    [InlineData(289, new byte[] { 22, 21, 4, 2, 3, 15, 43 })]
    public void EnqueueRequest__Serialization(int key, byte[] payload)
    {
        AssertBase(new EnqueueRequest(key, payload));
    }

    [Theory(DisplayName = nameof(BatchRequest))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    public void BatchRequest__Serialization(int requestsCount)
    {
        var requests = Enumerable.Range(0, requestsCount)
                                 .Select(x => CreateRandomRequest())
                                 .ToArray();
        AssertBase(new BatchRequest(requests));
    }
    
    private static readonly RequestType[] RequestTypes = new[]
    {
        RequestType.DequeueRequest, RequestType.EnqueueRequest, 
        RequestType.GetCountRequest,
        RequestType.BatchRequest
    };
    
    private IJobQueueRequest CreateRandomRequest()
    {
        var requestType = RequestTypes[Random.Shared.Next(0, RequestTypes.Length)];
        return requestType switch
               {
                   RequestType.EnqueueRequest => new EnqueueRequest(1, new byte[] {1, 2, 6, 75, 32}),
                   RequestType.DequeueRequest => DequeueRequest.Instance,
                   RequestType.GetCountRequest => GetCountRequest.Instance,
                   RequestType.BatchRequest => new BatchRequest(Enumerable.Range(0, 3).Select(_ => CreateRandomRequest()).ToArray()),
                   _ => throw new ArgumentOutOfRangeException()
               };
    }
}