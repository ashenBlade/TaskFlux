using System.Buffers;
using TaskFlux.Network.Requests.Authorization;
using TaskFlux.Network.Requests.Packets;

namespace TaskFlux.Network.Requests.Serialization.Tests;

public class PoolingNetworkPacketSerializerTests
{
    private static async Task AssertBase(Packet expected)
    {
        var stream = new MemoryStream();
        var serializerVisitor = new PoolingNetworkPacketSerializer(ArrayPool<byte>.Shared, stream);
        await expected.AcceptAsync(serializerVisitor);
        stream.Position = 0;
        var actual = await serializerVisitor.DeserializeAsync();
        Assert.Equal(expected, actual, PacketEqualityComparer.Instance);
    }
    
    [Theory(Timeout = 50)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(60)]
    [InlineData(100)]
    [InlineData(200)]
    [InlineData(byte.MaxValue)]
    [InlineData(byte.MaxValue + 1)]
    [InlineData(short.MaxValue)]
    public async Task CommandRequest__Serialization(int bufferSize)
    {
        var buffer = CreateRandomBuffer(bufferSize);
        await AssertBase(new CommandRequestPacket(buffer));
    }

    private static byte[] CreateRandomBuffer(int bufferSize)
    {
        var buffer = new byte[bufferSize];
        var random = new Random();
        for (var i = 0; i < buffer.Length; i++)
        {
            buffer[i] = ( byte ) random.Next(byte.MinValue, byte.MaxValue + 1);
        }

        return buffer;
    }

    [Theory(Timeout = 50)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(100)]
    [InlineData(byte.MaxValue)]
    [InlineData(byte.MaxValue + 1)]
    [InlineData(byte.MaxValue - 1)]
    [InlineData(short.MaxValue)]
    [InlineData(short.MaxValue + 1)]
    [InlineData(short.MaxValue - 1)]
    public async Task CommandResponse__Serialization(int bufferSize)
    {
        var buffer = CreateRandomBuffer(bufferSize);
        await AssertBase(new CommandResponsePacket(buffer));
    }

    [Theory]
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
    public async Task ErrorResponse__Serialization(string message)
    {
        await AssertBase(new ErrorResponsePacket(message));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(12)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    [InlineData(123)]
    [InlineData(-1)]
    [InlineData(-2)]
    [InlineData(23)]
    [InlineData(45)]
    public async Task NotLeaderResponse__Serialization(int id)
    {
        await AssertBase(new NotLeaderPacket(id));
    }

    public static IEnumerable<object[]> AuthorizationMethods = new[]
    {
        new object[]
        {
            new NoneAuthorizationMethod()
        }
    };

    [Theory]
    [MemberData(nameof(AuthorizationMethods))]
    public async Task AuthorizationRequest__Serialization(AuthorizationMethod method)
    {
        await AssertBase(new AuthorizationRequestPacket(method));
    }

    [Fact]
    public async Task AuthorizationResponse__Success__Serialization()
    {
        await AssertBase(AuthorizationResponsePacket.Ok);
    }

    [Theory]
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
}