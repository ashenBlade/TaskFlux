using System.Net;
using TaskFlux.Core;
using TaskFlux.Domain;
using TaskFlux.Network;
using TaskFlux.Network.Authorization;
using TaskFlux.Network.Commands;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Responses;
using TaskFlux.Network.Responses.Policies;

namespace TaskFlux.Transport.Tcp.Common.Tests;

// ReSharper disable StringLiteralTypo
[Trait("Category", "Serialization")]
public class PacketTests
{
    private static async Task AssertBase(Packet expected)
    {
        var stream = new MemoryStream();
        await expected.SerializeAsync(stream, CancellationToken.None);
        stream.Position = 0;
        var actual = await Packet.DeserializeAsync(stream, CancellationToken.None);
        Assert.Equal(expected, actual, PacketEqualityComparer.Instance);
    }

    public static IEnumerable<object[]> NetworkCommands => new[]
    {
        new NetworkCommand[] { new CountNetworkCommand(QueueName.Default) },
        new NetworkCommand[] { new DequeueNetworkCommand(QueueName.Default, 0) },
        new NetworkCommand[] { new EnqueueNetworkCommand(QueueName.Parse("asdf"), 123, "hello, world"u8.ToArray()) },
        new NetworkCommand[] { new CreateQueueNetworkCommand(QueueName.Parse("asdf"), 1, null, null, null) },
        new NetworkCommand[] { new DeleteQueueNetworkCommand(QueueName.Parse("@@@@@")) },
        new NetworkCommand[] { new ListQueuesNetworkCommand() }
    };

    [Theory(DisplayName = nameof(CommandRequestPacket))]
    [MemberData(nameof(NetworkCommands))]
    public async Task CommandRequest__Serialization(NetworkCommand command)
    {
        await AssertBase(new CommandRequestPacket(command));
    }

    public static IEnumerable<object[]> NetworkResponses => new NetworkResponse[][]
    {
        [new CountNetworkResponse(123)],
        [new DequeueNetworkResponse(new QueueRecord(new RecordId(1), 123, "asdfasdf"u8.ToArray()))],
        [new ErrorNetworkResponse(2, "какая-то ошибка")],
        [new PolicyViolationNetworkResponse(new GenericNetworkQueuePolicy("sample"))],
        [new ListQueuesNetworkResponse(Array.Empty<ITaskQueueInfo>())],
    };

    [Theory(DisplayName = nameof(CommandResponsePacket))]
    [MemberData(nameof(NetworkResponses))]
    public async Task CommandResponse__Serialization(NetworkResponse response)
    {
        await AssertBase(new CommandResponsePacket(response));
    }

    [Theory(DisplayName = nameof(ErrorResponsePacket))]
    [InlineData("")]
    [InlineData("Необработанное исключение")]
    [InlineData("Invalid data requested")]
    [InlineData("Version incompatible")]
    [InlineData("1")]
    [InlineData(" ")]
    [InlineData("\0")]
    [InlineData("\n")]
    [InlineData("\n\r\r\n\0")]
    [InlineData("hello, world!")]
    [InlineData("\0   \n\r\t")]
    [InlineData("\\asdfasdfasdfasdf")]
    [InlineData("what? ")]
    [InlineData("Ошибка на строне сервера. Код: 123")]
    public async Task ErrorResponse__Message__Serialization(string message)
    {
        await AssertBase(new ErrorResponsePacket(1, message));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public async Task ErrorResponse__ErrorCode__Serialization(int code)
    {
        await AssertBase(new ErrorResponsePacket(code, string.Empty));
    }

    [Theory(DisplayName = nameof(NotLeaderPacket))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(12)]
    [InlineData(23)]
    [InlineData(45)]
    [InlineData(123)]
    [InlineData(int.MaxValue)]
    public async Task NotLeaderPacket__Serialization(int id)
    {
        await AssertBase(new NotLeaderPacket(id));
    }

    [Fact(DisplayName = nameof(NotLeaderPacket) + "_null")]
    public async Task NotLeaderPacket__Null__Serialization()
    {
        await AssertBase(new NotLeaderPacket(null));
    }

    public static IEnumerable<object[]> AuthorizationMethods = new[] { new object[] { new NoneAuthorizationMethod() } };

    [Theory(DisplayName = nameof(AuthorizationRequestPacket))]
    [MemberData(nameof(AuthorizationMethods))]
    public async Task AuthorizationRequest__Serialization(AuthorizationMethod method)
    {
        await AssertBase(new AuthorizationRequestPacket(method));
    }

    [Fact(DisplayName = nameof(AuthorizationResponsePacket) + "_Успех")]
    public async Task AuthorizationResponse__Success__Serialization()
    {
        await AssertBase(AuthorizationResponsePacket.Ok);
    }

    [Theory(DisplayName = nameof(AuthorizationResponsePacket) + "_Ошибка")]
    [InlineData("")]
    [InlineData("\0")]
    [InlineData("\n")]
    [InlineData("    ")]
    [InlineData("Something went wrong")]
    [InlineData(@"Unhandled exception. System.Exception: Ошибка доступа к файлу
    at Program.<Main>$(String[] args) in /home/user/projects/sample/Program.cs:line 9")]
    [InlineData("hello, world!")]
    [InlineData("\n\rфыва\0а\nфаasdfdsf   213223 $!@ &щ&& ))(HVDm,,.Sfdфыва")]
    public async Task AuthorizationResponse__Error__Serialization(string errorReason)
    {
        await AssertBase(AuthorizationResponsePacket.Error(errorReason));
    }

    [Theory(DisplayName = nameof(BootstrapRequestPacket))]
    [InlineData(0, 0, 0)]
    [InlineData(0, 0, 1)]
    [InlineData(1, 0, 0)]
    [InlineData(1, 1, 1)]
    [InlineData(1, 2, 3)]
    [InlineData(10, 2, 32)]
    [InlineData(0, 5, 0)]
    [InlineData(2, 2, 5)]
    public async Task BootstrapRequest__Serialization(int major, int minor, int patch)
    {
        await AssertBase(new BootstrapRequestPacket(major, minor, patch));
    }

    [Fact(DisplayName = nameof(BootstrapResponsePacket) + "_Успех")]
    public async Task BootstrapResponse__Success__Serialization()
    {
        await AssertBase(BootstrapResponsePacket.Ok);
    }

    [Theory(DisplayName = nameof(BootstrapResponsePacket) + "_Ошибка")]
    [InlineData("")]
    [InlineData("\0")]
    [InlineData("\n")]
    [InlineData("    ")]
    [InlineData("Something went wrong")]
    [InlineData(@"Unhandled exception. System.Exception: Ошибка доступа к файлу
    at Program.<Main>$(String[] args) in /home/user/projects/sample/Program.cs:line 9")]
    [InlineData("hello, world!")]
    [InlineData("\n\rфыва\0а\nфаasdfdsf   213223 $!@ &щ&& ))(HVDm,,.Sfdфыва")]
    [InlineData("Версия сервера не согласуется с версией клиента. Версия сервера: 2.0.1. Версия клиента: 1.9.2")]
    public async Task BootstrapResponse__Error__Serialization(string message)
    {
        await AssertBase(BootstrapResponsePacket.Error(message));
    }

    public static IEnumerable<object> GetClusterMetadataResponse => new object[]
    {
        new object[] { new EndPoint[] { new DnsEndPoint("hello.world.ru", 9000) }, 0, 0 },
        new object?[]
        {
            new EndPoint[]
            {
                new IPEndPoint(IPAddress.Parse("123.123.123.123"), 2602), new DnsEndPoint("tflux.1", 2622)
            },
            new int?(), 1
        },
        new object[]
        {
            new EndPoint[]
            {
                new DnsEndPoint("tflux.1", 2602), new DnsEndPoint("tflux.2", 2602),
                new DnsEndPoint("tflux.3", 2602)
            },
            0, 2
        },
        new object?[]
        {
            new EndPoint[]
            {
                new DnsEndPoint("tflux.1", 2602), new DnsEndPoint("tflux.2", 2602),
                new IPEndPoint(IPAddress.Parse("192.168.34.10"), 3444)
            },
            new int?(), 1
        }
    };

    [Theory(DisplayName = nameof(ClusterMetadataResponsePacket))]
    [MemberData(nameof(GetClusterMetadataResponse))]
    public async Task ClusterMetadataResponse__Serialization(EndPoint[] endPoints, int? leaderId, int respondingId)
    {
        await AssertBase(new ClusterMetadataResponsePacket(endPoints, leaderId, respondingId));
    }

    [Fact(DisplayName = nameof(ClusterMetadataRequestPacket))]
    public async Task ClusterMetadataRequest__Serialization()
    {
        await AssertBase(new ClusterMetadataRequestPacket());
    }

    [Fact(DisplayName = nameof(AcknowledgeRequestPacket))]
    public async Task AcknowledgeRequest__Serialization()
    {
        await AssertBase(new AcknowledgeRequestPacket());
    }

    [Fact(DisplayName = nameof(NegativeAcknowledgeRequestPacket))]
    public async Task NegativeAcknowledgeRequest__Serialization()
    {
        await AssertBase(new NegativeAcknowledgeRequestPacket());
    }

    [Fact(DisplayName = nameof(OkPacket))]
    public async Task OkPacket__Serialization()
    {
        await AssertBase(new OkPacket());
    }
}