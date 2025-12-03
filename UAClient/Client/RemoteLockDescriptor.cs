using Opc.Ua;

namespace UAClient.Client
{
    public class RemoteLockDescriptor
    {
        public NodeId BaseNodeId { get; set; }
        // Methods
        public NodeId? InitLock { get; set; }
        public NodeId? BreakLock { get; set; }
        public NodeId? ExitLock { get; set; }
        public NodeId? RenewLock { get; set; }
        // Variables
        public NodeId? Locked { get; set; }
        public NodeId? LockingUser { get; set; }
        public NodeId? LockingClient { get; set; }
        public NodeId? RemainingLockTime { get; set; }
        public NodeId? CurrentState { get; set; }
    }
}
