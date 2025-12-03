using System;
using System.Threading.Tasks;
using Opc.Ua;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class RemoteLockTests
    {
        [Fact]
        public async Task HasMethodAsync_ReturnsFalse_WhenNoSession()
        {
            var rl = new RemoteLock("L", new NodeId(1));
            var result = await rl.HasMethodAsync(null, "InitLock");
            Assert.False(result);
        }

        [Fact]
        public void Constructor_SetsProperties()
        {
            var rl = new RemoteLock("Lock1", new NodeId(5));
            Assert.Equal("Lock1", rl.Name);
            Assert.Equal(new NodeId(5), rl.BaseNodeId);
        }
    }
}
