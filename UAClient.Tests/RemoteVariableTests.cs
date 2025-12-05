using System;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class RemoteVariableTests
    {
        [Fact]
        public void UpdateFromDataValue_UpdatesValueAndTimestamp()
        {
            var nodeId = new Opc.Ua.NodeId("ns=1;s=Test");
            var rv = new RemoteVariable("TestVar", nodeId);
            var dv = new Opc.Ua.DataValue(new Opc.Ua.Variant(42)) { ServerTimestamp = DateTime.UtcNow };

            rv.UpdateFromDataValue(dv);

            Assert.Equal(42, rv.Value);
            Assert.NotNull(rv.Timestamp);
        }

        [Fact]
        public void WriteValue_ThrowsIfNoClient()
        {
            var nodeId = new Opc.Ua.NodeId("ns=1;s=Test");
            var rv = new RemoteVariable("TestVar", nodeId);
            Assert.Throws<InvalidOperationException>(() => rv.WriteValue(1));
        }
    }
}
