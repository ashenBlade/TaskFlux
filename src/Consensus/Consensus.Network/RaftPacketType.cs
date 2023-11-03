namespace Consensus.Network;

public enum RaftPacketType : byte
{
    AppendEntriesRequest = ( byte ) 'A',
    AppendEntriesResponse = ( byte ) 'a',

    ConnectRequest = ( byte ) 'C',
    ConnectResponse = ( byte ) 'c',

    RequestVoteRequest = ( byte ) 'V',
    RequestVoteResponse = ( byte ) 'v',

    InstallSnapshotRequest = ( byte ) 'S',
    InstallSnapshotChunk = ( byte ) 'b',
    InstallSnapshotResponse = ( byte ) 's',

    RetransmitRequest = ( byte ) 'R',
}