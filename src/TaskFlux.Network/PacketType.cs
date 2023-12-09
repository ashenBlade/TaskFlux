namespace TaskFlux.Network;

public enum PacketType : byte
{
    CommandRequest = ( byte ) 'C',
    CommandResponse = ( byte ) 'c',

    AcknowledgeRequest = ( byte ) 'K',
    NegativeAcknowledgementRequest = ( byte ) 'N',
    Ok = ( byte ) 'k',

    ErrorResponse = ( byte ) 'e',
    NotLeader = ( byte ) 'l',

    AuthorizationRequest = ( byte ) 'A',
    AuthorizationResponse = ( byte ) 'a',

    BootstrapRequest = ( byte ) 'B',
    BootstrapResponse = ( byte ) 'b',

    ClusterMetadataRequest = ( byte ) 'M',
    ClusterMetadataResponse = ( byte ) 'm',
}