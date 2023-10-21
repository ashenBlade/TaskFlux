namespace Consensus.Peer.Exceptions;

/// <summary>
/// Исключение возникающее когда обнаруживается ошибка при проверки целостности полученного пакета.
/// Обычно, это означает несоответствие отправленной и рассчитанной чек-суммы
/// </summary>
public class IntegrityException : Exception
{
    public override string Message => "Обнаружено нарушение целостности при получении пакета";
}