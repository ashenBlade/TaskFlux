namespace TaskFlux.Network.Requests;

public enum PacketType: byte
{
    DataRequest = (byte)'D',
    DataResponse = (byte)'d',
    
    ErrorResponse = (byte)'e',
    NotLeader = (byte) 'l',
}