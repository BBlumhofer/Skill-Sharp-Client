using System;
using System.Threading.Tasks;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class UaClientTests
    {
        [Fact]
        public void Constructor_SetsUrl()
        {
            var c = new UaClient("opc.tcp://example:4840", "u", "p");
            Assert.NotNull(c);
        }

        [Fact]
        public async Task ReadNodeAsync_WithoutSession_Throws()
        {
            var c = new UaClient("opc.tcp://example:4840");
            await Assert.ThrowsAsync<InvalidOperationException>(() => c.ReadNodeAsync("ns=1;s=Node"));
        }
    }
}
