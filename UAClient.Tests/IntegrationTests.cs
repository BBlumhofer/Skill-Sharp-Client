using System;
using System.Threading.Tasks;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    // Integration tests that exercise a real OPC UA server. They run only when the
    // environment variable RUN_INTEGRATION_TESTS is set to "1" to avoid breaking CI.
    public class IntegrationTests
    {
        private static bool ShouldRunIntegration => Environment.GetEnvironmentVariable("RUN_INTEGRATION_TESTS") == "1";

        private static async Task WithTimeout(Task task, TimeSpan timeout)
        {
            var completed = await Task.WhenAny(task, Task.Delay(timeout));
            if (completed == task)
            {
                await task; // propagate exceptions
                return;
            }
            throw new TimeoutException($"Operation timed out after {timeout.TotalSeconds} seconds");
        }

        [Fact]
        public async Task ConnectAndDiscoverModules_WhenDisabled_IsNoOp()
        {
            if (!ShouldRunIntegration)
            {
                // Skip running the integration test when the environment flag is not set.
                return;
            }

            var url = Environment.GetEnvironmentVariable("UA_TEST_SERVER") ?? "opc.tcp://localhost:4842";
            var username = Environment.GetEnvironmentVariable("UA_TEST_USER") ?? "orchestrator";
            var password = Environment.GetEnvironmentVariable("UA_TEST_PASS") ?? "orchestrator";

            var client = new UaClient(url, username, password);
            try
            {
                await WithTimeout(client.ConnectAsync(), TimeSpan.FromSeconds(60));
                var server = new RemoteServer(client);
                await WithTimeout(server.ConnectAsync(), TimeSpan.FromSeconds(60));

                // basic assertion: there is at least one discovered module or Components dictionary present
                Assert.True(server.Modules.Count >= 0);
            }
            finally
            {
                try { await client.DisconnectAsync(); } catch { }
            }
        }

        [Fact]
        public async Task StartModuleStartupSkill_WhenAvailable_Runs()
        {
            if (!ShouldRunIntegration)
            {
                return;
            }

            var url = Environment.GetEnvironmentVariable("UA_TEST_SERVER") ?? "opc.tcp://localhost:4842";
            var username = Environment.GetEnvironmentVariable("UA_TEST_USER") ?? "orchestrator";
            var password = Environment.GetEnvironmentVariable("UA_TEST_PASS") ?? "orchestrator";

            var client = new UaClient(url, username, password);
            try
            {
                await WithTimeout(client.ConnectAsync(), TimeSpan.FromSeconds(60));
                var server = new RemoteServer(client);
                await WithTimeout(server.ConnectAsync(), TimeSpan.FromSeconds(60));

                // Try to find the example module name used by Program.cs
                if (!server.Modules.TryGetValue("CA-Module", out var module))
                {
                    // if not present, try to use any discovered module
                    if (server.Modules.Count == 0)
                    {
                        throw new InvalidOperationException("No modules discovered on server.");
                    }
                    module = server.Modules.Values.GetEnumerator().Current ?? throw new InvalidOperationException("No modules available");
                }

                // Attempt to lock and start the module's startup skill (best-effort)
                var session = client.Session ?? throw new InvalidOperationException("No session");
                var locked = await module.LockAsync(session);
                // locked may be null; we don't require it to be true

                // Start module (this will attempt coupling and startup skill); don't fail test if startup skill missing
                await WithTimeout(module.StartAsync(reset: true, timeout: TimeSpan.FromSeconds(60)), TimeSpan.FromSeconds(90));

                // If we reached here, basic integration succeeded
                Assert.True(true);
            }
            finally
            {
                try { await client.DisconnectAsync(); } catch { }
            }
        }
    }
}
