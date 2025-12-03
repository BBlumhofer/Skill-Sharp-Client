using System;
using System.Linq;
using System.Threading.Tasks;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class ModuleLifecycleTests
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
        public async Task MakeReady_MakesModuleReady_WhenServerSupportsLifecycle()
        {
            if (!ShouldRunIntegration) return;

            var url = Environment.GetEnvironmentVariable("UA_TEST_SERVER") ?? "opc.tcp://localhost:4842";
            var username = Environment.GetEnvironmentVariable("UA_TEST_USER") ?? "orchestrator";
            var password = Environment.GetEnvironmentVariable("UA_TEST_PASS") ?? "orchestrator";

            var client = new UaClient(url, username, password);
            try
            {
                await WithTimeout(client.ConnectAsync(), TimeSpan.FromSeconds(60));
                var server = new RemoteServer(client);
                await WithTimeout(server.ConnectAsync(), TimeSpan.FromSeconds(60));

                // find a module
                RemoteModule module = null;
                if (!server.Modules.TryGetValue("CA-Module", out var mm))
                {
                    if (server.Modules.Count == 0) Assert.True(false, "No modules discovered on server");
                    module = server.Modules.Values.First();
                }
                else module = mm;

                Assert.NotNull(module);

                // Attempt to make ready (lock + couple + startup)
                var ok = await module.MakeReadyAsync(TimeSpan.FromSeconds(120));

                // Evaluate readiness and assert
                await module.EvaluateReadyAsync();
                Assert.True(module.IsReady, "Module did not reach ready state after MakeReadyAsync");
            }
            finally
            {
                try { await client.DisconnectAsync(); } catch { }
            }
        }

        [Fact]
        public async Task RemoteStorage_IsInStorage_FindsItem_WhenSlotHasProductId()
        {
            if (!ShouldRunIntegration) return;

            var url = Environment.GetEnvironmentVariable("UA_TEST_SERVER") ?? "opc.tcp://localhost:4842";
            var username = Environment.GetEnvironmentVariable("UA_TEST_USER") ?? "orchestrator";
            var password = Environment.GetEnvironmentVariable("UA_TEST_PASS") ?? "orchestrator";

            var client = new UaClient(url, username, password);
            try
            {
                await WithTimeout(client.ConnectAsync(), TimeSpan.FromSeconds(60));
                var server = new RemoteServer(client);
                await WithTimeout(server.ConnectAsync(), TimeSpan.FromSeconds(60));

                // find a module that exposes storages
                RemoteModule module = null;
                if (!server.Modules.TryGetValue("CA-Module", out var mm))
                {
                    if (server.Modules.Count == 0) return; // nothing to test
                    module = server.Modules.Values.First();
                }
                else module = mm;

                if (module.Storages.Count == 0) return; // nothing to test here

                var storage = module.Storages.Values.First();

                // If no slots discovered try to subscribe/read variables on storage to find product identifiers
                // Look for a slot that has a ProductID or CarrierID variable with a value
                string? foundProductId = null;
                foreach (var slot in storage.Slots.Values)
                {
                    // ensure client refs are set and try to read ProductID/CarrierID
                    try
                    {
                        if (slot.Variables.TryGetValue("ProductID", out var rv) && rv != null)
                        {
                            // attempt to read live value via session
                            try
                            {
                                var dv = await client.Session.ReadValueAsync(rv.NodeId, default);
                                rv.UpdateFromDataValue(dv);
                                if (rv.Value != null) { foundProductId = rv.Value.ToString(); break; }
                            }
                            catch { }
                        }

                        if (slot.Variables.TryGetValue("CarrierID", out var rv2) && rv2 != null)
                        {
                            try
                            {
                                var dv = await client.Session.ReadValueAsync(rv2.NodeId, default);
                                rv2.UpdateFromDataValue(dv);
                                if (rv2.Value != null) { foundProductId = rv2.Value.ToString(); break; }
                            }
                            catch { }
                        }
                    }
                    catch { }
                }

                if (string.IsNullOrEmpty(foundProductId))
                {
                    // attempt to scan storage variables directly for a ProductID
                    foreach (var kv in storage.Variables)
                    {
                        try
                        {
                            if (string.Equals(kv.Key, "ProductID", StringComparison.OrdinalIgnoreCase) && kv.Value != null)
                            {
                                var dv = await client.Session.ReadValueAsync(kv.Value.NodeId, default);
                                kv.Value.UpdateFromDataValue(dv);
                                if (kv.Value.Value != null) { foundProductId = kv.Value.Value.ToString(); break; }
                            }
                        }
                        catch { }
                    }
                }

                if (string.IsNullOrEmpty(foundProductId))
                {
                    // nothing readable to assert against; skip
                    return;
                }

                var (found, slotFound) = await storage.IsInStorageAsync(client, productId: foundProductId);
                Assert.True(found, "IsInStorageAsync did not report the known product present in storage");
                Assert.NotNull(slotFound);
            }
            finally
            {
                try { await client.DisconnectAsync(); } catch { }
            }
        }
    }
}
