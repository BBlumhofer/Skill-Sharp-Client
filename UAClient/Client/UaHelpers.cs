using System;
using Opc.Ua;
using Opc.Ua.Client;

namespace UAClient.Client
{
    public static class UaHelpers
    {
        public static NodeId ToNodeId(ExpandedNodeId expanded, Session session)
        {
            if (expanded == null) return NodeId.Null;
            if (!expanded.IsAbsolute)
            {
                // Prefer mapping by NamespaceUri if provided to get the server's namespace index
                ushort ns = 0;
                try
                {
                    if (!string.IsNullOrEmpty(expanded.NamespaceUri))
                    {
                        int idx = session.NamespaceUris.GetIndex(expanded.NamespaceUri);
                        if (idx >= 0) ns = (ushort)idx;
                        else ns = (ushort)expanded.NamespaceIndex;
                    }
                    else
                    {
                        ns = (ushort)expanded.NamespaceIndex;
                    }
                }
                catch
                {
                    ns = (ushort)expanded.NamespaceIndex;
                }

                var id = expanded.Identifier;
                if (id is uint || id is int || id is ushort || id is short || id is ulong || id is long)
                {
                    // convert to uint for numeric NodeId constructor
                    return new NodeId(Convert.ToUInt32(id), ns);
                }
                else if (id is string)
                {
                    return new NodeId((string)id, ns);
                }
                else if (id is Guid)
                {
                    return new NodeId((Guid)id, ns);
                }
                else if (id is byte[])
                {
                    return new NodeId((byte[])id, ns);
                }

                // Fallback: try generic constructor
                try
                {
                    return new NodeId(id, ns);
                }
                catch
                {
                    return NodeId.Null;
                }
            }
            throw new NotSupportedException("Absolute ExpandedNodeId is not supported by this helper");
        }
    }
}
