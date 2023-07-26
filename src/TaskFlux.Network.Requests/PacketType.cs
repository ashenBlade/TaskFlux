namespace TaskFlux.Network.Requests;

public enum PacketType: byte
{
    CommandRequest = (byte)'C',
    CommandResponse = (byte)'c',
    
    ErrorResponse = (byte)'e',
    NotLeader = (byte) 'l',
    
    // TODO: добавить объекты пакетов с такими типами + алгоритм установления соединения
    AuthorizationRequest = (byte)'A',
    AuthorizationResponse = (byte)'a',
}