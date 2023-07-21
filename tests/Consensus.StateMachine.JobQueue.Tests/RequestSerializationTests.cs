using TaskFlux.Requests;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Requests.JobQueue.Enqueue;
using TaskFlux.Requests.Serialization;

namespace Consensus.StateMachine.JobQueue.Tests;

public class RequestSerializationTests
{
    private static readonly RequestSerializer Serializer = new();

    private static void AssertBase(IRequest expected)
    {
        using var memory = new MemoryStream();
        using var writer = new BinaryWriter(memory);
        
        var payload = Serializer.Serialize(expected);
        var actual = Serializer.Deserialize(payload);
        
        Assert.Equal(expected, actual, RequestEqualityComparer.Instance);
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
                                 .Select(_ => CreateRandomRequest())
                                 .ToArray();
        AssertBase(new BatchRequest(requests));
    }
    
    private static readonly RequestType[] RequestTypes = new[]
    {
        RequestType.DequeueRequest, RequestType.EnqueueRequest, 
        RequestType.GetCountRequest,
        RequestType.BatchRequest
    };
    
    private static IRequest CreateRandomRequest()
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

    [Fact(DisplayName = $"{nameof(BatchRequest)} (Вложенный)")]
    public void NestedBatchRequest__ДолженДесериализоватьИсходныйЗапрос()
    {
        var request = new BatchRequest(new List<IRequest>()
        {
            new BatchRequest(new List<IRequest>()
            {
                new EnqueueRequest(123, new byte[] {1, 2, 3, 4, 5, byte.MaxValue, byte.MinValue}),
                new BatchRequest(new List<IRequest>() {new GetCountRequest()}),
            }),
            new BatchRequest(new List<IRequest>()
            {
                new DequeueRequest(),
                new DequeueRequest(),
                new GetCountRequest(),
            }),
            new EnqueueRequest(5465, new byte[] {8, 34, 23, 76, 55, 99, 100})
        });
        AssertBase(request);
    }
}