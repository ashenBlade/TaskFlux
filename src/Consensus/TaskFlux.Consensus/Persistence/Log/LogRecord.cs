namespace TaskFlux.Consensus.Persistence.Log;

// TODO: потом _index заменить на такое, чтобы было appendPosition проще находить + чек-сумму иногда проверять
/// <summary>
/// 
/// </summary>
/// <param name="Term"></param>
/// <param name="CheckSum"></param>
/// <param name="Length"></param>
public record struct LogRecord(Term Term, int CheckSum, int Length);