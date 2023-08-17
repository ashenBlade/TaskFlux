using System.Runtime.Serialization;

namespace Consensus.Core;

public interface ISerializer<TCommand>
{
    /// <summary>
    /// Сериализовать переденный объект команды в массив байт
    /// </summary>
    /// <param name="command">Команда для сериализации</param>
    /// <returns>Сериализованное представление команды</returns>
    /// <exception cref="SerializationException">Во время сериализации возникли ошибки</exception>
    public byte[] Serialize(TCommand command);
    
    /// <summary>
    /// Десериализовать переданный массив байт в объект команды
    /// </summary>
    /// <param name="payload">Массив байт, представляющий сериализованную команду</param>
    /// <returns>Десериализованная команда</returns>
    /// <exception cref="SerializationException">Во время десериализации возникли ошибки</exception>
    public TCommand Deserialize(byte[] payload);
}