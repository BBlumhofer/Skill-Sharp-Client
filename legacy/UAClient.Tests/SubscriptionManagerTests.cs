using System;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class SubscriptionManagerTests
    {
        [Fact]
        public async Task AddMonitoredItemAsync_Throws_WhenNoSession()
        {
            var client = new UaClient("opc.tcp://example:4840");
            var mgr = new SubscriptionManager(client);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await mgr.AddMonitoredItemAsync(new NodeId(1), (m,e)=>{}));
        }

        [Fact]
        public async Task SubscribeDataChangeAsync_Throws_WhenNoSession()
        {
            var client = new UaClient("opc.tcp://example:4840");
            var mgr = new SubscriptionManager(client);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await mgr.SubscribeDataChangeAsync(new DummyHandler(), new [] { new NodeId(1) }));
        }

        private class DummyHandler : IDataChangeHandler
        {
            public void DataChangeNotification(NodeId nodeId, DataValue value, MonitoredItemNotificationEventArgs args) { }
        }
    }
}
