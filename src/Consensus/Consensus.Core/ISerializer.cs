namespace Consensus.Core;

public interface ISerializer<TCommand>
{
    public byte[] Serialize(TCommand command);
    public TCommand Deserialize(byte[] payload);
}