using System;
using Opc.Ua;

namespace UAClient.Client
{
    public class RemoteStorageSlot
    {
        public string Name { get; }
        public NodeId BaseNodeId { get; }
        public IDictionary<string, RemoteVariable> Variables { get; } = new Dictionary<string, RemoteVariable>();
        public IDictionary<NodeId, RemoteVariable> NodeMap { get; } = new Dictionary<NodeId, RemoteVariable>();

        public RemoteStorageSlot(string name, NodeId baseNodeId)
        {
            Name = name;
            BaseNodeId = baseNodeId;
        }

        // Convenience properties similar to the Python RemoteStorageSlot
        public string Type => "StorageSlot";

        public string? CarrierId
        {
            get
            {
                if (Variables.TryGetValue("CarrierID", out var v) && v?.Value != null) return v.Value.ToString();
                return null;
            }
        }

        public object? CarrierType
        {
            get
            {
                if (Variables.TryGetValue("CarrierType", out var v))
                {
                    if (v?.Value == null) return NodeId.Null;
                    return v.Value;
                }
                return NodeId.Null;
            }
        }

        public string? ProductId
        {
            get { if (Variables.TryGetValue("ProductID", out var v) && v?.Value != null) return v.Value.ToString(); return null; }
        }

        public bool? IsSlotEmpty
        {
            get
            {
                if (Variables.TryGetValue("IsSlotEmpty", out var v) && v?.Value != null)
                {
                    if (v.Value is bool b) return b;
                    if (v.Value is int i) return i != 0;
                    if (v.Value is uint ui) return ui != 0;
                    if (bool.TryParse(v.Value.ToString(), out var pb)) return pb;
                }
                return null;
            }
        }

        public bool? IsCarrierEmpty
        {
            get
            {
                if (Variables.TryGetValue("IsCarrierEmpty", out var v) && v?.Value != null)
                {
                    if (v.Value is bool b) return b;
                    if (v.Value is int i) return i != 0;
                    if (v.Value is uint ui) return ui != 0;
                    if (bool.TryParse(v.Value.ToString(), out var pb)) return pb;
                }
                return null;
            }
        }

        public object? ProductType
        {
            get
            {
                if (Variables.TryGetValue("ProductType", out var v))
                {
                    if (v?.Value == null) return NodeId.Null;
                    return v.Value;
                }
                return NodeId.Null;
            }
        }

        // Raw state value (may be string or numeric)
        public object? State
        {
            get { if (Variables.TryGetValue("CurrentState", out var v)) return v?.Value; return null; }
        }
    }
}
