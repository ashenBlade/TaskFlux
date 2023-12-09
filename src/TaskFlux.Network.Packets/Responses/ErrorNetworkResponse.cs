using Utils.Serialization;

namespace TaskFlux.Network.Packets.Responses;

public class ErrorNetworkResponse : NetworkResponse
{
    public int ErrorType { get; }
    public string Message { get; }
    public override NetworkResponseType Type => NetworkResponseType.Error;

    public ErrorNetworkResponse(int errorType, string message)
    {
        ErrorType = errorType;
        Message = message;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var writer = new StreamBinaryWriter(stream);
        await writer.WriteAsync(( byte ) NetworkResponseType.Error, token);
        await writer.WriteAsync(ErrorType, token);
        await writer.WriteAsync(Message, token);
    }

    public new static async ValueTask<ErrorNetworkResponse> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var type = await reader.ReadInt32Async(token);
        var message = await reader.ReadStringAsync(token);
        return new ErrorNetworkResponse(type, message);
    }

    public override T Accept<T>(INetworkResponseVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }
}