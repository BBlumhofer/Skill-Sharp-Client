using System;
using Opc.Ua;
using System.Threading.Tasks;

namespace UAClient.Client
{
    public class RemoteStorage
    {
        public string Name { get; }
        public NodeId BaseNodeId { get; }
        public IDictionary<string, RemoteVariable> Variables { get; } = new Dictionary<string, RemoteVariable>();
        public IDictionary<NodeId, RemoteVariable> NodeMap { get; } = new Dictionary<NodeId, RemoteVariable>();
        public IDictionary<string, RemoteStorageSlot> Slots { get; } = new Dictionary<string, RemoteStorageSlot>(StringComparer.OrdinalIgnoreCase);

        public RemoteStorage(string name, NodeId baseNodeId)
        {
            Name = name;
            BaseNodeId = baseNodeId;
        }

        // Expose current state as a property (read from Variables["CurrentState"] or similar)
        public object? CurrentState
        {
            get
            {
                if (Variables.TryGetValue("CurrentState", out var stateVar))
                {
                    return stateVar.Value;
                }
                if (Variables.TryGetValue("State", out var altStateVar))
                {
                    return altStateVar.Value;
                }
                return null;
            }
        }

        public async Task SetupSubscriptionsAsync(SubscriptionManager? subscriptionManager, UaClient? client = null)
        {
            if (subscriptionManager == null && client != null)
            {
                try { subscriptionManager = new SubscriptionManager(client); } catch { subscriptionManager = null; }
            }

            if (subscriptionManager != null)
            {
                foreach (var rv in Variables.Values)
                {
                    try { await rv.SetupSubscriptionAsync(subscriptionManager); } catch { }
                }
                // also subscribe variables of discovered slots
                foreach (var slot in Slots.Values)
                {
                    try
                    {
                        foreach (var rv in slot.Variables.Values)
                        {
                            try { if (client != null) rv.SetClient(client); } catch { }
                            try { await rv.SetupSubscriptionAsync(subscriptionManager); } catch { }
                        }
                    }
                    catch { }
                }
            }
        }

        // Check whether a product/carrier is in storage. Caller can provide one of the identifiers.
        // If a UaClient is provided, missing variable values will be read from the server as fallback.
        public async Task<(bool found, RemoteStorageSlot? slot)> IsInStorageAsync(UaClient? client = null, string? productId = null, NodeId? productType = null, string? carrierId = null, NodeId? carrierType = null)
        {
            // If no search criteria provided, return false
            if (string.IsNullOrEmpty(productId) && productType == null && string.IsNullOrEmpty(carrierId) && carrierType == null)
                return (false, null);

            foreach (var slot in Slots.Values)
            {
                try
                {
                    bool match = true;

                    // helper to get variable value (cached or via read)
                    async Task<object?> GetVarAsync(string key)
                    {
                        if (slot.Variables.TryGetValue(key, out var rv) && rv != null)
                        {
                            if (rv.Value != null) return rv.Value;
                            if (client != null)
                            {
                                try
                                {
                                    var dv = await client.Session.ReadValueAsync(rv.NodeId, System.Threading.CancellationToken.None);
                                    rv.UpdateFromDataValue(dv);
                                    return rv.Value;
                                }
                                catch { }
                            }
                        }
                        return null;
                    }

                    if (!string.IsNullOrEmpty(productId))
                    {
                        var v = await GetVarAsync("ProductID");
                        var s = v?.ToString();
                        if (!string.Equals(s, productId, StringComparison.OrdinalIgnoreCase)) match = false;
                    }
                    if (productType != null && match)
                    {
                        var v = await GetVarAsync("ProductType");
                        if (v == null) match = false;
                        else
                        {
                            // compare NodeId string representations if types
                            if (v is NodeId nid)
                            {
                                if (!nid.Equals(productType)) match = false;
                            }
                            else
                            {
                                if (!string.Equals(v.ToString(), productType.ToString(), StringComparison.OrdinalIgnoreCase)) match = false;
                            }
                        }
                    }
                    if (!string.IsNullOrEmpty(carrierId) && match)
                    {
                        var v = await GetVarAsync("CarrierID");
                        var s = v?.ToString();
                        if (!string.Equals(s, carrierId, StringComparison.OrdinalIgnoreCase)) match = false;
                    }
                    if (carrierType != null && match)
                    {
                        var v = await GetVarAsync("CarrierType");
                        if (v == null) match = false;
                        else
                        {
                            if (v is NodeId nid)
                            {
                                if (!nid.Equals(carrierType)) match = false;
                            }
                            else
                            {
                                if (!string.Equals(v.ToString(), carrierType.ToString(), StringComparison.OrdinalIgnoreCase)) match = false;
                            }
                        }
                    }

                    if (match) return (true, slot);
                }
                catch { }
            }
            return (false, null);
        }
    }
}
