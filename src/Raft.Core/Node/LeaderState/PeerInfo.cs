namespace Raft.Core.Node.LeaderState;

/// <summary>
/// Информация об узле, необходимая для взаимодействия с ним в состоянии <see cref="NodeRole.Leader"/>
/// </summary>
public class PeerInfo
{
    /// <summary>
    /// Индекс следующей записи в логе, которую необходимо отправить клиенту
    /// </summary>
    public int NextIndex { get; private set; }

    /// <summary>
    /// Индекс последней зафиксированной (реплицированной) записи в логе
    /// </summary>
    public int MatchIndex { get; private set; }
    
    public PeerInfo(int nextIndex)
    {
        NextIndex = nextIndex;
        MatchIndex = 0;
    }

    /// <summary>
    /// Обновить информацию об имеющися на узле записям
    /// </summary>
    /// <param name="appliedCount">Количество успешно отправленных вхождений команд (Entries)</param>
    public void Update(int appliedCount)
    {
        var nextIndex = NextIndex + appliedCount;
        var matchIndex = nextIndex - 1;
        MatchIndex = matchIndex;
        NextIndex = nextIndex;
    }

    /// <summary>
    /// Отктиться назад, если узел ответил на AppendEntries <c>false</c>
    /// </summary>
    /// <exception cref="InvalidOperationException"><see cref="NextIndex"/> равен 0</exception>
    public void Decrement()
    {
        if (NextIndex is 0)
        {
            throw new InvalidOperationException("Нельзя откаться на индекс меньше 0");
        }

        NextIndex--;
    }
    
    public override string ToString() => $"PeerInfo(NextIndex = {NextIndex}, MatchIndex = {MatchIndex})";
}