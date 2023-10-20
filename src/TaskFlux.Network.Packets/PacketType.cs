namespace TaskFlux.Network.Packets;

public enum PacketType : byte
{
    CommandRequest = ( byte ) 'C',
    CommandResponse = ( byte ) 'c',

    ErrorResponse = ( byte ) 'e',
    NotLeader = ( byte ) 'l',

    AuthorizationRequest = ( byte ) 'A',
    AuthorizationResponse = ( byte ) 'a',

    BootstrapRequest = ( byte ) 'B',
    BootstrapResponse = ( byte ) 'b',

    ClusterMetadataRequest = ( byte ) 'M',
    ClusterMetadataResponse = ( byte ) 'm',
}