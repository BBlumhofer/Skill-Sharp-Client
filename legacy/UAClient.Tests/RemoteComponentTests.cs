using System;
using Opc.Ua;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class RemoteComponentTests
    {
        private class TestSubscriber : IRemoteComponentSubscriber
        {
            public string? LastState;
            public void OnStateChange(string? newState)
            {
                LastState = newState;
            }

            public void OnNewMessage(RemoteComponent component, string text, int severity, string code)
            {
                // not used in this test
            }
        }

        [Fact]
        public void DataChangeNotification_NotifiesSubscriber_OnMonitoringNode()
        {
            var client = new UaClient("opc.tcp://localhost:4840"); // not connected, not used in this test
            var remoteServer = new RemoteServer(client);
            var rc = new RemoteComponent("comp", new NodeId(1000, 2), client, remoteServer);

            var rv = new RemoteVariable("CurrentState", new NodeId(2000, 2));
            rc.RegisterMonitoringNode(rv);

            var sub = new TestSubscriber();
            rc.AddSubscriber(sub);

            // simulate a data change: set CurrentState to a string value
            var dv = new DataValue(new Variant("MyState"));
            rc.DataChangeNotification(rv.NodeId, dv, null);

            Assert.Equal("MyState", sub.LastState);
        }
    }
}
