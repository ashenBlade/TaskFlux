namespace Consensus.Network;

public interface IPacket
{
    public PacketType PacketType { get; }
    
    /// <summary>
    /// Оценить размер требуемый для сериализации этого пакета.
    /// Полученное значение должно быть оптимальным рамером буфера для сериализации пакета
    /// </summary>
    /// <returns>Рамер пакета в байтах</returns>
    /// <remarks>При рассчете учитываеются как полезная нагрузка, так и рабочие заголовки.
    /// Например, байт маркера тоже учитывается</remarks>
    public int EstimatePacketSize();
}