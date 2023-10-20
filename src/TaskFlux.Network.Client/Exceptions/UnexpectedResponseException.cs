using TaskFlux.Network.Packets;

namespace TaskFlux.Network.Client.Exceptions;

/// <summary>
/// Исключение, возникающее при неожиданном ответе от сервера TaskFlux
/// </summary>
public class UnexpectedResponseException : TaskFluxException
{
    public PacketType Received { get; }
    public PacketType? Expected { get; }

    public UnexpectedResponseException(PacketType received)
    {
        Received = received;
    }

    public UnexpectedResponseException(PacketType received, PacketType expected)
    {
        Received = received;
        Expected = expected;
    }

    public override string Message => Expected is null
                                          ? $"От сервера получен неожиданный пакет данных: {Received}"
                                          : $"От сервера получен неожиданный пакет данных: {Received}. Ожидался: {Expected.Value}";
}