namespace TaskFlux.Consensus.Network.Message;

public enum NodePacketType : byte
{
    AppendEntriesRequest = (byte)'A',
    AppendEntriesResponse = (byte)'a',

    ConnectRequest = (byte)'C',
    ConnectResponse = (byte)'c',

    RequestVoteRequest = (byte)'V',
    RequestVoteResponse = (byte)'v',

    InstallSnapshotRequest = (byte)'S',
    InstallSnapshotResponse = (byte)'s',
    InstallSnapshotChunkRequest = (byte)'B',
    InstallSnapshotChunkResponse = (byte)'b',

    RetransmitRequest = (byte)'R',
}