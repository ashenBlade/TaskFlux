namespace TaskFlux.Commands.Serialization.Exceptions;

/// <summary>
/// Исключение, возникающее при десериализации неизвестной политики (неизвестный код политики)
/// </summary>
public class UnknownPolicyException : DeserializationException
{
    /// <summary>
    /// Полученный код политики
    /// </summary>
    public int PolicyCode { get; }

    public override string Message => $"Во время десериализации обнаружен неизвестный код политики: {PolicyCode}";

    public UnknownPolicyException(int policyCode)
    {
        PolicyCode = policyCode;
    }
}