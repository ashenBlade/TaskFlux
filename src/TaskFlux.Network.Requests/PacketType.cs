namespace TaskFlux.Network.Requests;

public enum PacketType: byte
{
    CommandRequest = (byte)'C',
    CommandResponse = (byte)'c',
    
    ErrorResponse = (byte)'e',
    NotLeader = (byte) 'l',
    
    AuthorizationRequest = (byte)'A',
    AuthorizationResponse = (byte)'a',
    
    BootstrapRequest = (byte)'B',
    BootstrapResponse = (byte)'b',
}