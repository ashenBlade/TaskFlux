using TaskFlux.Network.Packets.Authorization;

namespace TaskFlux.Network.Packets.Exceptions;

/// <summary>
/// Исключение, возникающее когда получен неизвестный тип <see cref="AuthorizationMethod"/>
/// </summary>
public class UnknownAuthorizationMethodException : Exception
{
    /// <summary>
    /// Неизвестный маркер типа авторизации
    /// </summary>
    public byte AuthorizationType { get; }

    public UnknownAuthorizationMethodException(byte authorizationType)
    {
        AuthorizationType = authorizationType;
    }
}