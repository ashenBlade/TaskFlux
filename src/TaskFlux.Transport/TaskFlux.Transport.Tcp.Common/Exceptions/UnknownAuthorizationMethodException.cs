using TaskFlux.Network.Authorization;

namespace TaskFlux.Network.Exceptions;

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