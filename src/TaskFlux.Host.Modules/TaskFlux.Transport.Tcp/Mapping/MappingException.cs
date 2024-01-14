using TaskFlux.Commands.Error;

namespace TaskFlux.Transport.Tcp.Mapping;

/// <summary>
/// Базовое исключение, возникающее при ошибке маппинга 
/// </summary>
public class MappingException : Exception
{
    /// <summary>
    /// Код бизнес-ошибки
    /// </summary>
    public ErrorType ErrorCode { get; }

    public MappingException(ErrorType errorCode)
    {
        ErrorCode = errorCode;
    }
}