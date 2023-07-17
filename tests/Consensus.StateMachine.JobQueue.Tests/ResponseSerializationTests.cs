using System.ComponentModel;
using Consensus.StateMachine.JobQueue.Commands;
using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.Error;
using Consensus.StateMachine.JobQueue.Commands.GetCount;
using Consensus.StateMachine.JobQueue.Serialization;

namespace Consensus.StateMachine.JobQueue.Tests;

public class ResponseSerializationTests
{
    public static readonly JobQueueResponseSerializer Serializer = new();
    public static readonly JobQueueResponseDeserializer Deserializer = new();
    
    private static void AssertBase(IJobQueueResponse expected)
    {
        using var memory = new MemoryStream();
        using var writer = new BinaryWriter(memory);
        Serializer.Serialize(expected, writer);
        var actual = Deserializer.Deserialize(memory.ToArray());
        Assert.Equal(expected, actual, JobQueueResponseEqualityComparer.Instance);
    }

    [Theory(DisplayName = nameof(EnqueueResponse))]
    [InlineData(true)]
    [InlineData(false)]
    public void EnqueueResponse__Serialization(bool success)
    {
        AssertBase(new EnqueueResponse(success));
    }

    [Theory]
    [InlineData(true, 1, new byte[]{1, 2, 3})]
    [InlineData(true, 1, new byte[]{byte.MaxValue})]
    [InlineData(true, 90, new byte[]{byte.MinValue})]
    [InlineData(true, 2, new byte[]{34, 1, 23, 0, 2})]
    [InlineData(false, 0, new byte[]{34, 1, 23, 0, 2})]
    [InlineData(false, 0, new byte[]{})]
    [InlineData(true, 0, new byte[]{})]
    [InlineData(true, int.MaxValue, new byte[]{})]
    [InlineData(true, int.MinValue, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1, 0})]
    [InlineData(false, int.MinValue, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1, 0})]
    [InlineData(false, 12312421, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1, 0})]
    [InlineData(true, -85531324, new byte[]{byte.MaxValue, byte.MinValue})]
    [InlineData(true, 43, new byte[]{byte.MaxValue, byte.MinValue, 0, byte.MaxValue})]
    public void DequeueResponse__Serialization(bool success, int key, byte[] payload)
    {
        AssertBase(new DequeueResponse(success, key, payload));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    [InlineData(10)]
    [InlineData(5)]
    [InlineData(123123)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(876544)]
    public void GetCountResponse__Serialization(int count)
    {
        AssertBase(new GetCountResponse(count));
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData(" ")]
    [InlineData("\n\r")]
    [InlineData("hello, world!")]
    [InlineData("русский текст")]
    [InlineData("привет, мир")]
    [InlineData("\"enqueue\" command must contain key and payload to add")]
    [InlineData("Authorization error")]
    [InlineData("Max payload length exceeded")]
    [InlineData("Request must contain single line")]
    [InlineData("!№;№(?*:;?")]
    [InlineData("\b")]
    [InlineData("\r")]
    [InlineData("\0after zero")]
    [InlineData("\0after zero character")]
    [InlineData("\'\\n\\r")]
    public void ErrorResponse__Serialization(string message)
    {
        AssertBase(new ErrorResponse(message));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    public void BatchResponse__Serialization(int responsesCount)
    {
        AssertBase(new BatchResponse(CreateResponses()));
        
        IJobQueueResponse[] CreateResponses()
        {
            return new IJobQueueResponse[]
            {
                CreateDequeueResponse(),
                CreateEnqueueResponse(),
                CreateBatchResponse(),
                CreateErrorResponse(),
                CreateGetCountResponse()
            }.Cycle(responsesCount)
             .ToArray();

            DequeueResponse CreateDequeueResponse() => new(true, 1, Array.Empty<byte>());
            EnqueueResponse CreateEnqueueResponse() => new(true);
            ErrorResponse CreateErrorResponse() => new("Custom error message");
            GetCountResponse CreateGetCountResponse() => new(10);
            BatchResponse CreateBatchResponse() =>
                new(new IJobQueueResponse[]
                {
                    CreateEnqueueResponse(),
                    CreateDequeueResponse(),
                    CreateErrorResponse(),
                    CreateGetCountResponse()
                });
        }
    }
}