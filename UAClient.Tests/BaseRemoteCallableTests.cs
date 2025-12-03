using System;
using System.Collections.Generic;
using Opc.Ua;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class BaseRemoteCallableTests
    {
        private class TestCallable : BaseRemoteCallable
        {
            public TestCallable(string name, NodeId baseNodeId) : base(name, baseNodeId, new UaClient("opc.tcp://localhost:4840")) { }

            public void AddFinalResultNode(NodeId id, RemoteVariable rv)
            {
                _final_result_nodes[id] = rv;
            }

            public void AddMonitoringNode(NodeId id, RemoteVariable rv)
            {
                _monitoring_nodes[id] = rv;
            }

            // expose protected dictionaries for assertions in tests
            public IReadOnlyDictionary<NodeId, RemoteVariable> FinalResultNodes => _final_result_nodes as IReadOnlyDictionary<NodeId, RemoteVariable>;
            public IReadOnlyDictionary<NodeId, RemoteVariable> MonitoringNodes => _monitoring_nodes as IReadOnlyDictionary<NodeId, RemoteVariable>;
        }

        [Fact]
        public void DataChangeNotification_UpdatesFinalResult_AndNotifiesSubscriber()
        {
            var nodeId = new NodeId(100);
            var rv = new RemoteVariable("final", nodeId);
            var call = new TestCallable("test", new NodeId(1));
            call.AddFinalResultNode(nodeId, rv);

            bool called = false;
            RemoteVariable? received = null;
            call.AddSubscriber(r => { called = true; received = r; });

            var dv = new DataValue(new Variant("value1"));
            call.DataChangeNotification(nodeId, dv, null);

            Assert.True(called, "Subscriber should be called for final result updates");
            Assert.NotNull(received);
            Assert.Equal("value1", received!.Value);
        }

        [Fact]
        public void DataChangeNotification_UpdatesMonitoring_WithoutNotifyingSubscriber()
        {
            var nodeId = new NodeId(200);
            var rv = new RemoteVariable("mon", nodeId);
            var call = new TestCallable("test2", new NodeId(2));
            call.AddMonitoringNode(nodeId, rv);

            bool called = false;
            call.AddSubscriber(r => { called = true; });

            var dv = new DataValue(new Variant(12345));
            call.DataChangeNotification(nodeId, dv, null);

            Assert.False(called, "Subscriber should not be called for monitoring updates");
            Assert.Equal(12345, rv.Value);
        }
    }
}
