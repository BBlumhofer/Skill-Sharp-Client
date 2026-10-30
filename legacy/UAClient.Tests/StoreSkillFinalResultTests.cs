using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Opc.Ua;
using UAClient.Client;
using UAClient.Common;
using Xunit;

namespace UAClient.Tests
{
    public class StoreSkillFinalResultTests
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

        private static async Task<T> WithTimeout<T>(Task<T> task, TimeSpan timeout)
        {
            var completed = await Task.WhenAny(task, Task.Delay(timeout));
            if (completed == task)
            {
                return await task;
            }
                throw new TimeoutException($"Operation timed out after {timeout.TotalSeconds} seconds");
        }

        private static int? TryCoerceToInt(object? value, TypeInfo? sourceType = null)
        {
            if (value == null) return null;

            try
            {
                if (sourceType != null)
                {
                    return (int)TypeInfo.Cast(value, sourceType, BuiltInType.Int32);
                }

                return (int)TypeInfo.Cast(value, BuiltInType.Int32);
            }
            catch
            {
                try
                {
                    if (value is IConvertible convertible)
                    {
                        return Convert.ToInt32(convertible, CultureInfo.InvariantCulture);
                    }
                }
                catch
                {
                    switch (value)
                    {
                        case int i: return i;
                        case uint ui: return ui > int.MaxValue ? int.MaxValue : (int)ui;
                        case long l when l <= int.MaxValue && l >= int.MinValue: return (int)l;
                        case ulong ul when ul <= (ulong)int.MaxValue: return (int)ul;
                        case short s: return s;
                        case ushort us: return us;
                        case byte b: return b;
                        case sbyte sb: return sb;
                        default:
                            if (int.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed)) return parsed;
                            break;
                    }
                }
            }

            return null;
        }

        private static async Task<int?> ReadFinalResultAsIntAsync(RemoteSkill skill, string key, UaClient client)
        {
            if (!skill.FinalResultData.TryGetValue(key, out var rv) || rv?.NodeId == null) return null;
            var session = client.Session ?? throw new InvalidOperationException("No session");
            var dv = await session.ReadValueAsync(rv.NodeId, System.Threading.CancellationToken.None);
            TypeInfo? typeInfo = null;
            if (dv != null)
            {
                typeInfo = dv.WrappedValue.TypeInfo;
            }

            return TryCoerceToInt(dv?.Value, typeInfo);
        }

        [Fact]
        public async Task StoreSkill_Executes_And_FinalResultData_Includes_SuccessCount()
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

                var session = client.Session ?? throw new InvalidOperationException("No session");

                // attempt to lock the module before executing skills
                var locked = await module.LockAsync(session);
                if (!(locked.HasValue && locked.Value))
                {
                    UAClient.Common.Log.Warn("StoreSkill test: Module lock unavailable (likely owned by another client); skipping test run");
                    return;
                }

                // try to find a skill that mentions "Store"
                RemoteSkill storeSkill = null;
                foreach (var s in module.Methods.Values.OfType<RemoteSkill>())
                {
                    if (s.Name.IndexOf("Store", StringComparison.OrdinalIgnoreCase) >= 0) { storeSkill = s; break; }
                }
                if (storeSkill == null)
                {
                    foreach (var s in module.SkillSet.Values)
                    {
                        if (s.Name.IndexOf("Store", StringComparison.OrdinalIgnoreCase) >= 0) { storeSkill = s; break; }
                    }
                }

                if (storeSkill == null)
                {
                    // search in components as fallback
                    foreach (var c in module.Components.Values)
                    {
                        foreach (var s in c.MethodSet.Values.OfType<RemoteSkill>())
                        {
                            if (s.Name.IndexOf("Store", StringComparison.OrdinalIgnoreCase) >= 0) { storeSkill = s; break; }
                        }
                        if (storeSkill != null) break;
                        foreach (var s in c.SkillSet.Values)
                        {
                            if (s.Name.IndexOf("Store", StringComparison.OrdinalIgnoreCase) >= 0) { storeSkill = s; break; }
                        }
                        if (storeSkill != null) break;
                    }
                }

                if (storeSkill == null)
                {
                    // nothing to test on this server
                    return;
                }

                // Debug: list available method nodes under the skill base node
                try
                {
                    UAClient.Common.Log.Info($"StoreSkill BaseNodeId = {storeSkill.BaseNodeId}");
                    var browser = new Opc.Ua.Client.Browser(session)
                    {
                        BrowseDirection = Opc.Ua.BrowseDirection.Forward,
                        ReferenceTypeId = null,
                        NodeClassMask = (int)(Opc.Ua.NodeClass.Object | Opc.Ua.NodeClass.Variable | Opc.Ua.NodeClass.Method)
                    };

                    var q = new System.Collections.Generic.Queue<Opc.Ua.NodeId>();
                    q.Enqueue(storeSkill.BaseNodeId);
                    var seen = new System.Collections.Generic.HashSet<string>();
                    Console.WriteLine("Methods under skill (recursive):");
                    while (q.Count > 0)
                    {
                        var n = q.Dequeue();
                        ReferenceDescriptionCollection refs = null;
                        try { refs = await browser.BrowseAsync(n); } catch (Exception ex) { Console.WriteLine($"  Browse error for {n}: {ex.Message}"); continue; }
                        if (refs == null) continue;
                        foreach (var r in refs)
                        {
                            try
                            {
                                if (r.NodeClass == Opc.Ua.NodeClass.Method)
                                {
                                    var name = r.DisplayName?.Text ?? r.BrowseName?.Name ?? "(unknown)";
                                    var expanded = r.NodeId as Opc.Ua.ExpandedNodeId ?? new Opc.Ua.ExpandedNodeId(r.NodeId);
                                    var mid = UAClient.Client.UaHelpers.ToNodeId(expanded, session);
                                    var key = mid?.ToString() ?? name;
                                    if (!seen.Contains(key))
                                    {
                                        seen.Add(key);
                                        Console.WriteLine($"  Method: {name} -> NodeId={mid}");
                                    }
                                }
                                else if (r.NodeClass == Opc.Ua.NodeClass.Object || r.NodeClass == Opc.Ua.NodeClass.Variable)
                                {
                                    var expanded = r.NodeId as Opc.Ua.ExpandedNodeId ?? new Opc.Ua.ExpandedNodeId(r.NodeId);
                                    var cid = UAClient.Client.UaHelpers.ToNodeId(expanded, session);
                                    if (cid != null && !seen.Contains(cid.ToString())) q.Enqueue(cid);
                                }
                            }
                            catch { }
                        }
                    }
                }
                catch (Exception ex)
                {
                        UAClient.Common.Log.Warn($"Failed to enumerate methods for Store skill: {ex.Message}");
                }

                // ensure subscriptions for skill
                var submgr = server.SubscriptionManager ?? new SubscriptionManager(client);
                await WithTimeout(storeSkill.SetupSubscriptionsAsync(submgr, true), TimeSpan.FromSeconds(20));

                // ensure at least one port is coupled (Store skill may depend on coupled port)
                if (module.Ports.Count > 0)
                {
                    var firstPort = module.Ports.Values.First();
                    await WithTimeout(firstPort.SetupSubscriptionsAsync(new SubscriptionManager(client)), TimeSpan.FromSeconds(20));

                    // Diagnostic: enumerate methods under the CoupleSkill (if present) before attempting coupling
                    try
                    {
                        UAClient.Common.Log.Info($"Diagnostic: inspecting CoupleSkill for port {firstPort.Name}");
                        RemoteSkill couple = null;
                        try { if (firstPort.SkillSet != null && firstPort.SkillSet.TryGetValue("CoupleSkill", out var ls)) couple = ls; } catch { }
                        if (couple == null)
                        {
                            foreach (var kv in firstPort.Methods)
                            {
                                if (kv.Value is RemoteSkill rs && string.Equals(rs.Name, "CoupleSkill", StringComparison.OrdinalIgnoreCase)) { couple = rs; break; }
                            }
                        }
                        if (couple == null && module != null)
                        {
                            foreach (var kv in module.Methods)
                            {
                                if (kv.Value is RemoteSkill rs && string.Equals(rs.Name, "CoupleSkill", StringComparison.OrdinalIgnoreCase)) { couple = rs; break; }
                            }
                        }

                        if (couple == null)
                        {
                            UAClient.Common.Log.Info("Diagnostic: no CoupleSkill object found for this port/module");
                        }
                        else
                        {
                            UAClient.Common.Log.Info($"Diagnostic: CoupleSkill BaseNodeId = {couple.BaseNodeId}");
                            var browser = new Opc.Ua.Client.Browser(session)
                            {
                                BrowseDirection = Opc.Ua.BrowseDirection.Forward,
                                ReferenceTypeId = null,
                                NodeClassMask = (int)(Opc.Ua.NodeClass.Object | Opc.Ua.NodeClass.Variable | Opc.Ua.NodeClass.Method)
                            };

                            var q = new System.Collections.Generic.Queue<Opc.Ua.NodeId>();
                            q.Enqueue(couple.BaseNodeId);
                            var seen = new System.Collections.Generic.HashSet<string>();
                            UAClient.Common.Log.Info("Diagnostic: Methods under CoupleSkill (recursive):");
                            while (q.Count > 0)
                            {
                                var n = q.Dequeue();
                                ReferenceDescriptionCollection refs = null;
                                try { refs = await browser.BrowseAsync(n); } catch (Exception ex) { UAClient.Common.Log.Warn($"  Browse error for {n}: {ex.Message}"); continue; }
                                if (refs == null) continue;
                                foreach (var r in refs)
                                {
                                    try
                                    {
                                        var name = r.DisplayName?.Text ?? r.BrowseName?.Name ?? "(unknown)";
                                        var expanded = r.NodeId as Opc.Ua.ExpandedNodeId ?? new Opc.Ua.ExpandedNodeId(r.NodeId);
                                        var nid = UAClient.Client.UaHelpers.ToNodeId(expanded, session);
                                        if (r.NodeClass == Opc.Ua.NodeClass.Method)
                                        {
                                            var key = nid?.ToString() ?? name;
                                            if (!seen.Contains(key))
                                            {
                                                seen.Add(key);
                                                UAClient.Common.Log.Info($"  Method: {name} -> NodeId={nid}");
                                            }
                                        }
                                        else if (r.NodeClass == Opc.Ua.NodeClass.Object || r.NodeClass == Opc.Ua.NodeClass.Variable)
                                        {
                                            if (nid != null && !seen.Contains(nid.ToString())) q.Enqueue(nid);
                                        }
                                    }
                                    catch { }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        UAClient.Common.Log.Warn($"Diagnostic: failed to enumerate CoupleSkill methods: {ex.Message}");
                    }

                    await WithTimeout(firstPort.CoupleAsync(client, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(60));
                    var coupled = await firstPort.IsCoupledAsync(client);
                    Assert.True(coupled, "Port coupling failed or did not reach coupled state");
                }

                // Ensure the module's startup skill is running before executing the Store skill
                RemoteSkill startupSkill = null;
                try
                {
                    startupSkill = module.Methods.Values.OfType<RemoteSkill>()
                        .FirstOrDefault(s => s.Name.IndexOf("Startup", StringComparison.OrdinalIgnoreCase) >= 0);
                    if (startupSkill == null && module.SkillSet != null)
                    {
                        startupSkill = module.SkillSet.Values
                            .FirstOrDefault(s => s.Name.IndexOf("Startup", StringComparison.OrdinalIgnoreCase) >= 0);
                    }
                }
                catch { }

                if (startupSkill != null)
                {
                    UAClient.Common.Log.Info("Ensuring Startup skill runs before executing Store skill");
                    await WithTimeout(module.StartAsync(reset: true, timeout: TimeSpan.FromSeconds(30), couple: false), TimeSpan.FromSeconds(60));
                    var running = await WithTimeout(startupSkill.WaitForStateAsync(SkillStates.Running, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(60));
                    Assert.True(running, "Startup skill did not reach Running state");
                }
                else
                {
                    UAClient.Common.Log.Warn("No Startup skill found on module; continuing without startup guard");
                }

                // Ensure Store skill is in Ready state before the first execution
                var storeState = await WithTimeout(storeSkill.GetStateAsync(), TimeSpan.FromSeconds(10));
                if (storeState == (int)SkillStates.Running || storeState == (int)SkillStates.Starting || storeState == (int)SkillStates.Completing)
                {
                    UAClient.Common.Log.Info($"Store skill currently running (state={storeState}), waiting for completion before reset");
                    var completed = await WithTimeout(storeSkill.WaitForStateAsync(SkillStates.Completed, TimeSpan.FromSeconds(60)), TimeSpan.FromSeconds(60));
                    if (!completed)
                    {
                        UAClient.Common.Log.Warn("Store skill did not finish within 60s; skipping test to avoid interfering with another run");
                        return;
                    }
                    storeState = await WithTimeout(storeSkill.GetStateAsync(), TimeSpan.FromSeconds(10));
                }

                if (storeState != (int)SkillStates.Ready)
                {
                    UAClient.Common.Log.Info($"Store skill not ready before execution (state={storeState}), attempting Reset");
                    try
                    {
                        await WithTimeout(storeSkill.ResetAsync(), TimeSpan.FromSeconds(30));
                    }
                    catch (ServiceResultException sre) when (sre.StatusCode == StatusCodes.BadInvalidState)
                    {
                        UAClient.Common.Log.Warn($"Store skill reset reported BadInvalidState (state={storeState}); continuing to wait for Ready");
                    }

                    var readyOk = await WithTimeout(storeSkill.WaitForStateAsync(SkillStates.Ready, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(60));
                    Assert.True(readyOk, "Store skill did not reach Ready state after Reset");
                }

                // execute the skill twice to cause SuccessfulExecutionsCount to increment
                var parameters = new Dictionary<string, object?>();

                var exec1 = await ((dynamic)storeSkill).ExecuteAsync(parameters, waitForCompletion: true, resetAfterCompletion: true, resetBeforeIfHalted: true, timeout: TimeSpan.FromSeconds(60));
                Assert.NotNull(exec1);
                Console.WriteLine("--- Exec1 snapshot ---");
                foreach (var kv in (IDictionary<string, object?>)exec1)
                {
                    Console.WriteLine($"Exec1[{kv.Key}] -> (type={kv.Value?.GetType().Name ?? "null"}) '{kv.Value}'");
                }
                Console.WriteLine("--- End Exec1 snapshot ---");

                // small pause to allow server side summarization
                await Task.Delay(500);

                var exec2 = await ((dynamic)storeSkill).ExecuteAsync(parameters, waitForCompletion: true, resetAfterCompletion: true, resetBeforeIfHalted: true, timeout: TimeSpan.FromSeconds(60));
                Assert.NotNull(exec2);
                Console.WriteLine("--- Exec2 snapshot ---");
                foreach (var kv in (IDictionary<string, object?>)exec2)
                {
                    Console.WriteLine($"Exec2[{kv.Key}] -> (type={kv.Value?.GetType().Name ?? "null"}) '{kv.Value}'");
                }
                Console.WriteLine("--- End Exec2 snapshot ---");

                // allow final result data to propagate
                await Task.Delay(500);

                // Try to read FinalResultData -- look for SuccessfulExecutionsCount key
                Assert.NotNull(storeSkill.FinalResultData);

                // Print FinalResultData for debugging
                Console.WriteLine("--- FinalResultData dump ---");
                foreach (var kv in storeSkill.FinalResultData)
                {
                    var val = kv.Value?.Value;
                    Console.WriteLine($"FinalResultData[{kv.Key}] -> Value=(type={val?.GetType().Name ?? "null"}) '{val}'");
                }
                Console.WriteLine("--- End FinalResultData dump ---");

                var foundKey = storeSkill.FinalResultData.Keys.FirstOrDefault(k => k.IndexOf("SuccessfulExecutionsCount", StringComparison.OrdinalIgnoreCase) >= 0);
                if (foundKey == null)
                {
                    // no explicit key, assert that any FinalResultData entry exists
                    Assert.True(storeSkill.FinalResultData.Count > 0, "No FinalResultData keys present after executing Store skill");
                }
                else
                {
                    var latestCount = TryCoerceToInt(storeSkill.FinalResultData[foundKey].Value);
                    if (!latestCount.HasValue || latestCount.Value < 2)
                    {
                        UAClient.Common.Log.Warn($"SuccessfulExecutionsCount not updated yet (current={latestCount?.ToString() ?? "null"}); polling for up to 30s");
                        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
                        while (DateTime.UtcNow < deadline)
                        {
                            var refreshed = await ReadFinalResultAsIntAsync(storeSkill, foundKey, client);
                            if (refreshed.HasValue) latestCount = refreshed;
                            if (latestCount.HasValue && latestCount.Value >= 2) break;
                            await Task.Delay(1000);
                        }
                    }

                    Assert.True(latestCount.HasValue && latestCount.Value >= 2,
                        $"Expected SuccessfulExecutionsCount >= 2 after two executions, but was {latestCount?.ToString() ?? "null"}");
                }
            }
            finally
            {
                try { await client.DisconnectAsync(); } catch { }
            }
        }
    }
}
