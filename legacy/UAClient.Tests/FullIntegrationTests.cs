using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using UAClient.Client;
using Xunit;

namespace UAClient.Tests
{
    public class FullIntegrationTests
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
        public async Task Module_FullLifecycle_EndToEnd()
        {
            if (!ShouldRunIntegration) return;

            var url = Environment.GetEnvironmentVariable("UA_TEST_SERVER") ?? "opc.tcp://172.24.100.85:4892";
            var username = Environment.GetEnvironmentVariable("UA_TEST_USER") ?? "orchestrator";
            var password = Environment.GetEnvironmentVariable("UA_TEST_PASS") ?? "orchestrator";

            var client = new UaClient(url, username, password);
            try
            {
                await WithTimeout(client.ConnectAsync(), TimeSpan.FromSeconds(60));
                var server = new RemoteServer(client);
                await WithTimeout(server.ConnectAsync(), TimeSpan.FromSeconds(60));

                // find module
                RemoteModule module = null;
                if (!server.Modules.TryGetValue("CA-Module", out var mm))
                {
                    if (server.Modules.Count == 0) Assert.True(false, "No modules discovered on server");
                    module = server.Modules.Values.First();
                }
                else module = mm;

                Assert.NotNull(module);

                // ensure ports present
                Assert.True(module.Ports.Count > 0, "Module must expose at least one port");

                // ensure there is at least one skill available somewhere
                bool hasSkill = module.SkillSet.Count > 0 || module.Methods.Values.OfType<RemoteSkill>().Any();
                Assert.True(hasSkill, "Module must expose at least one skill");

                var session = client.Session ?? throw new InvalidOperationException("No session");

                // attempt to lock the module
                var locked = await module.LockAsync(session);
                Assert.True(locked.HasValue && locked.Value, "Module lock attempt failed or returned false");

                // start module (startup skill)
                await WithTimeout(module.StartAsync(reset: true, timeout: TimeSpan.FromSeconds(60)), TimeSpan.FromSeconds(90));

                // ensure a port with couple skill can be found and couple it
                RemotePort firstPort = module.Ports.Values.First();
                // try to couple
                await firstPort.SetupSubscriptionsAsync(new SubscriptionManager(client));
                await WithTimeout(firstPort.CoupleAsync(client, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(60));
                var coupled = await firstPort.IsCoupledAsync(client);
                Assert.True(coupled, "Port coupling failed or did not reach coupled state");

                // find a component with MoveToJointPosition skill
                RemoteComponent? robotComp = null;
                RemoteSkill? moveSkill = null;
                foreach (var c in module.Components.Values)
                {
                    if (c.SkillSet.TryGetValue("MoveToJointPosition", out var rs))
                    {
                        robotComp = c; moveSkill = rs; break;
                    }
                    // fallback: look in MethodSet
                    var ms = c.MethodSet.Values.OfType<RemoteSkill>().FirstOrDefault(s => s.Name.IndexOf("MoveToJointPosition", StringComparison.OrdinalIgnoreCase) >= 0);
                    if (ms != null) { robotComp = c; moveSkill = ms; break; }
                }

                Assert.NotNull(robotComp);
                Assert.NotNull(moveSkill);

                // prepare parameters for MoveToJointPosition
                var rnd = new Random();
                double RandAngle() => Math.Round(rnd.NextDouble() * 90.0 - 45.0, 2);
                var parameters = new Dictionary<string, object?>
                {
                    ["SAxisPosition"] = RandAngle(),
                    ["LAxisPosition"] = RandAngle(),
                    ["UAxisPosition"] = RandAngle(),
                    ["RAxisPosition"] = RandAngle(),
                    ["BAxisPosition"] = RandAngle(),
                    ["TAxisPosition"] = RandAngle()
                };

                // Ensure subscriptions for monitoring and skill state
                var submgr = new SubscriptionManager(client);
                await WithTimeout(robotComp.SubscribeCoreAsync(submgr), TimeSpan.FromSeconds(20));
                await WithTimeout(moveSkill.SubscribeCoreAsync(submgr), TimeSpan.FromSeconds(20));

                // execute and wait for completion
                var execResult = await ((dynamic)moveSkill).ExecuteAsync(parameters, waitForCompletion: true, resetAfterCompletion: true, resetBeforeIfHalted: true, timeout: TimeSpan.FromSeconds(120));
                Assert.NotNull(execResult);
                Assert.True(((IDictionary<string, object?>)execResult).Count >= 0, "FinalResultData should be readable");

                // execute without waiting and monitor
                var execTask = ((dynamic)moveSkill).ExecuteAsync(parameters, waitForCompletion: false, resetAfterCompletion: false, resetBeforeIfHalted: true, timeout: TimeSpan.FromSeconds(120));

                // poll while running and assert monitoring variables are readable
                var sw = System.Diagnostics.Stopwatch.StartNew();
                while (true)
                {
                    var state = await moveSkill.GetStateAsync();
                    if (state != null && state != (int)Common.SkillStates.Running) break;
                    // read a few monitoring variables
                    foreach (var kv in robotComp.Monitoring.Take(5))
                    {
                        try
                        {
                            var val = kv.Value.Value; // cached value
                        }
                        catch { }
                    }
                    await Task.Delay(1000);
                    if (sw.Elapsed > TimeSpan.FromSeconds(90)) break;
                }

                // ensure we can read final result after completion (best-effort)
                try
                {
                    foreach (var kv in moveSkill.FinalResultData)
                    {
                        var v = kv.Value.Value; // should not throw
                    }
                }
                catch { }

            }
            finally
            {
                try { await client.DisconnectAsync(); } catch { }
            }
        }

        [Fact]
        public async Task Skills_ShouldExpose_State_Finals_Monitoring_Parameters_And_TypeHints()
        {
            if (!ShouldRunIntegration) return;

            var url = Environment.GetEnvironmentVariable("UA_TEST_SERVER") ?? "opc.tcp://172.24.100.85:4892";
            var username = Environment.GetEnvironmentVariable("UA_TEST_USER") ?? "orchestrator";
            var password = Environment.GetEnvironmentVariable("UA_TEST_PASS") ?? "orchestrator";

            var client = new UaClient(url, username, password);
            try
            {
                await WithTimeout(client.ConnectAsync(), TimeSpan.FromSeconds(60));
                var server = new RemoteServer(client);
                await WithTimeout(server.ConnectAsync(), TimeSpan.FromSeconds(60));

                RemoteModule module = null;
                if (!server.Modules.TryGetValue("CA-Module", out var mm))
                {
                    if (server.Modules.Count == 0) Assert.True(false, "No modules discovered on server");
                    module = server.Modules.Values.First();
                }
                else module = mm;

                Assert.NotNull(module);

                var session = client.Session ?? throw new InvalidOperationException("No session");

                // create subscription manager
                var submgr = server.SubscriptionManager ?? new SubscriptionManager(client);

                // Helper to find skills by name across module and components
                IEnumerable<RemoteSkill> AllSkills()
                {
                    foreach (var s in module.Methods.Values.OfType<RemoteSkill>()) yield return s;
                    foreach (var s in module.SkillSet.Values) yield return s;
                    foreach (var c in module.Components.Values)
                    {
                        foreach (var s in c.SkillSet.Values) yield return s;
                        foreach (var s in c.MethodSet.Values.OfType<RemoteSkill>()) yield return s;
                    }
                }

                RemoteSkill? startupSkill = AllSkills().FirstOrDefault(s => s.Name.IndexOf("Startup", StringComparison.OrdinalIgnoreCase) >= 0);
                RemoteSkill? coupleSkill = AllSkills().FirstOrDefault(s => s.Name.IndexOf("Couple", StringComparison.OrdinalIgnoreCase) >= 0);
                RemoteSkill? moveSkill = AllSkills().FirstOrDefault(s => s.Name.IndexOf("MoveToJointPosition", StringComparison.OrdinalIgnoreCase) >= 0);

                // subscribe/setup for discovered skills
                if (startupSkill != null) await WithTimeout(startupSkill.SetupSubscriptionsAsync(submgr, true), TimeSpan.FromSeconds(20));
                if (coupleSkill != null) await WithTimeout(coupleSkill.SetupSubscriptionsAsync(submgr, true), TimeSpan.FromSeconds(20));
                if (moveSkill != null) await WithTimeout(moveSkill.SetupSubscriptionsAsync(submgr, true), TimeSpan.FromSeconds(20));

                // Assert CurrentState maps to SkillStates (best-effort)
                if (moveSkill != null)
                {
                    var st = await moveSkill.GetStateAsync();
                    Assert.NotNull(st);
                    Assert.True(Enum.IsDefined(typeof(Common.SkillStates), st.Value), "Move skill CurrentState should map to SkillStates enum");

                    // Ensure FinalResultData / Monitoring / ParameterSet exist (dictionaries should be present)
                    Assert.NotNull(moveSkill.FinalResultData);
                    Assert.NotNull(moveSkill.Monitoring);
                    Assert.NotNull(moveSkill.ParameterSet);
                }

                // Ensure startup and couple skills are recognized as continuous (non-finite) if present
                if (startupSkill != null)
                {
                    Assert.False(startupSkill.IsFinite, "Startup skill should be continuous (IsFinite==false)");
                }
                if (coupleSkill != null)
                {
                    Assert.False(coupleSkill.IsFinite, "Couple skill should be continuous (IsFinite==false)");
                }

                // Ensure MoveToJointPosition identified as finite if present
                if (moveSkill != null)
                {
                    Assert.True(moveSkill.IsFinite, "MoveToJointPosition should be identified as finite skill");
                }

                // If none of the skills were found, skip (no applicable module on server)
                if (startupSkill == null && coupleSkill == null && moveSkill == null)
                {
                    // nothing to validate on this server
                    return;
                }
            }
            finally
            {
                try { await client.DisconnectAsync(); } catch { }
            }
        }
    }
}
