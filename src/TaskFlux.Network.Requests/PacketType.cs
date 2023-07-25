namespace TaskFlux.Network.Requests;

public enum PacketType: byte
{
    CommandRequest = (byte)'C',
    CommandResponse = (byte)'c',
    
    ErrorResponse = (byte)'e',
    NotLeader = (byte) 'l',
}