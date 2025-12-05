using System;
using System.Collections.Generic;
using System.Linq;
using Opc.Ua;
using Opc.Ua.Client;
using System.Threading.Tasks;
using System.Diagnostics;
using UAClient.Common;

namespace UAClient.Client
{
    public class RemoteLock
    {
        public string Name { get; }
        public NodeId BaseNodeId { get; }
        // Convenience properties for commonly used lock variables (set during discovery)
        public RemoteVariable? Locked { get; set; }
        public RemoteVariable? LockingUser { get; set; }
        public RemoteVariable? LockingClient { get; set; }
        public RemoteVariable? RemainingLockTime { get; set; }
        public RemoteVariable? CurrentState { get; set; }
        public IDictionary<string, RemoteVariable> Variables { get; } = new Dictionary<string, RemoteVariable>(StringComparer.OrdinalIgnoreCase);
        public IDictionary<NodeId, RemoteVariable> NodeMap { get; } = new Dictionary<NodeId, RemoteVariable>();
        // Methods/skills attached to this lock during discovery
        public IDictionary<string, BaseRemoteCallable> Methods { get; } = new Dictionary<string, BaseRemoteCallable>(StringComparer.OrdinalIgnoreCase);
        public IDictionary<NodeId, BaseRemoteCallable> MethodsNodeMap { get; } = new Dictionary<NodeId, BaseRemoteCallable>();
        // Optional descriptor populated during discovery for standardized lock access
        public RemoteLockDescriptor? Descriptor { get; set; }

        // Convenience properties for commonly used lock methods (set during discovery)
        public BaseRemoteCallable? InitLock { get; set; }
        public BaseRemoteCallable? BreakLock { get; set; }
        public BaseRemoteCallable? ExitLock { get; set; }
        public BaseRemoteCallable? RenewLock { get; set; }
        public RemoteLock(string name, NodeId baseNodeId)
        {
            Name = name;
            BaseNodeId = baseNodeId;
        }

        public async Task EnsureSubscriptionsAsync(SubscriptionManager? subscriptionManager, UaClient? client = null)
        {
            if (subscriptionManager == null && client != null)
            {
                try { subscriptionManager = new SubscriptionManager(client); }
                catch { subscriptionManager = null; }
            }

            // subscribe to discovered variables
            if (subscriptionManager != null)
            {
                foreach (var kv in Variables.Values)
                {
                    try { await kv.SetupSubscriptionAsync(subscriptionManager); } catch { }
                }
            }

            // subscribe methods/skills attached to lock
            foreach (var m in Methods.Values)
            {
                try { await m.SetupSubscriptionsAsync(subscriptionManager, true); } catch { }
            }
        }

        public async Task<bool?> IsLockedAsync(Session session)
        {
            if (session == null) return null;

            // Descriptor-first: descriptor MUST point to a boolean per spec
            if (Descriptor != null && Descriptor.Locked != null)
            {
                try
                {
                    var dv = await session.ReadValueAsync(Descriptor.Locked, System.Threading.CancellationToken.None);
                    if (dv == null) return null;
                    if (dv.Value is bool b) return b;
                    throw new InvalidOperationException($"RemoteLock '{Name}': Descriptor.Locked did not return a boolean (actual={dv.Value?.GetType().Name ?? "null"})");
                }
                catch (ServiceResultException sre)
                {
                    UAClient.Common.Log.Warn($"RemoteLock.IsLockedAsync: read of Descriptor.Locked failed: {sre.Message}");
                    throw;
                }
            }

            // If discovery provided a convenience property, use it but require boolean type
            if (Locked != null)
            {
                try
                {
                    var dv = await session.ReadValueAsync(Locked.NodeId, System.Threading.CancellationToken.None);
                    if (dv == null) return null;
                    if (dv.Value is bool b) return b;
                    throw new InvalidOperationException($"RemoteLock '{Name}': 'Locked' variable is not boolean (actual={dv.Value?.GetType().Name ?? "null"})");
                }
                catch (ServiceResultException sre)
                {
                    UAClient.Common.Log.Warn($"RemoteLock.IsLockedAsync: read of Locked failed: {sre.Message}");
                    throw;
                }
            }

            // If neither descriptor nor convenience property exist, fail fast — discovery should populate these
            throw new InvalidOperationException($"RemoteLock '{Name}': required 'Locked' variable not discovered for {BaseNodeId}");
        }

        // Read lock owner information (LockingClient / LockingUser) if available. Returns string representation or null.
        public async Task<string?> GetLockOwnerAsync(Session session)
        {
            if (session == null) return null;
            try
            {
                // Prefer descriptor LockingClient if present
                if (Descriptor != null && Descriptor.LockingClient != null)
                {
                    UAClient.Common.Log.Debug($"GetLockOwnerAsync: reading Descriptor.LockingClient node={Descriptor.LockingClient}");
                    var dv = await session.ReadValueAsync(Descriptor.LockingClient, System.Threading.CancellationToken.None);
                    if (dv != null && dv.Value != null) return dv.Value.ToString();
                }
                // Fallback to convenience RemoteVariable fields discovered
                if (LockingClient != null)
                {
                    UAClient.Common.Log.Debug($"GetLockOwnerAsync: reading LockingClient NodeId={LockingClient.NodeId}");
                    var dv = await session.ReadValueAsync(LockingClient.NodeId, System.Threading.CancellationToken.None);
                    if (dv != null && dv.Value != null) return dv.Value.ToString();
                }
                if (Descriptor != null && Descriptor.LockingUser != null)
                {
                    UAClient.Common.Log.Debug($"GetLockOwnerAsync: reading Descriptor.LockingUser node={Descriptor.LockingUser}");
                    var dv = await session.ReadValueAsync(Descriptor.LockingUser, System.Threading.CancellationToken.None);
                    if (dv != null && dv.Value != null) return dv.Value.ToString();
                }
                if (LockingUser != null)
                {
                    UAClient.Common.Log.Debug($"GetLockOwnerAsync: reading LockingUser NodeId={LockingUser.NodeId}");
                    var dv = await session.ReadValueAsync(LockingUser.NodeId, System.Threading.CancellationToken.None);
                    if (dv != null && dv.Value != null) return dv.Value.ToString();
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Debug($"RemoteLock.GetLockOwnerAsync failed: {ex.Message}");
            }
            return null;
        }

        public async Task<bool> InitLockAsync(Session session)
        {
            if (session == null) throw new InvalidOperationException("No session");

            // Prefer explicit methods discovered during module discovery. Do not perform runtime recursive browse here.
            string[] candidates = new[] { "InitLock", "BreakLock", "RenewLock", "ExitLock", "AcquireLock", "EnterLock", "RequestLock", "Lock", "Init" };
            foreach (var cand in candidates)
            {
                try
                {
                    var matchKey = Methods.Keys.FirstOrDefault(k => k != null && k.IndexOf(cand, StringComparison.OrdinalIgnoreCase) >= 0);
                    if (matchKey == null) continue;
                    var callable = Methods[matchKey];
                    var methodEntry = MethodsNodeMap.FirstOrDefault(kv => object.ReferenceEquals(kv.Value, callable));
                    if (methodEntry.Key == null) continue;
                    var methodNodeId = methodEntry.Key;
                    UAClient.Common.Log.Info($"RemoteLock.InitLockAsync: calling discovered method '{matchKey}' ({methodNodeId}) on object {BaseNodeId}");
                    await session.CallAsync(BaseNodeId, methodNodeId, System.Threading.CancellationToken.None);
                    UAClient.Common.Log.Info($"RemoteLock.InitLockAsync: call of '{matchKey}' succeeded");
                    return true;
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteLock.InitLockAsync: call candidate failed: {ex.Message}");
                }
            }

            return false;
        }

        public async Task<bool> ReleaseLockAsync(Session session)
        {
            if (session == null) throw new InvalidOperationException("No session");
            string[] candidates = new[] { "ReleaseLock", "ExitLock", "Unlock", "Release", "LeaveLock" };
            foreach (var cand in candidates)
            {
                try
                {
                    var matchKey = Methods.Keys.FirstOrDefault(k => k != null && k.IndexOf(cand, StringComparison.OrdinalIgnoreCase) >= 0);
                    if (matchKey == null) continue;
                    var callable = Methods[matchKey];
                    var methodEntry = MethodsNodeMap.FirstOrDefault(kv => object.ReferenceEquals(kv.Value, callable));
                    if (methodEntry.Key == null) continue;
                    var methodNodeId = methodEntry.Key;
                    await session.CallAsync(BaseNodeId, methodNodeId, System.Threading.CancellationToken.None);
                    return true;
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Warn($"RemoteLock.ReleaseLockAsync: call candidate failed: {ex.Message}");
                }
            }

            return false;
        }

        public async Task<bool> WaitForLockedAsync(Session session, TimeSpan timeout)
        {
            if (session == null) throw new InvalidOperationException("No session");
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                try
                {
                    var state = await IsLockedAsync(session);
                    if (state.HasValue && state.Value) return true;
                }
                catch (Exception ex)
                {
                    UAClient.Common.Log.Debug($"RemoteLock.WaitForLockedAsync: IsLocked check failed: {ex.Message}");
                }
                await Task.Delay(500);
            }
            return false;
        }

        public async Task PrintVariablesAsync(Session session)
        {
            if (session == null) throw new InvalidOperationException("No session");
            try
            {
                Console.WriteLine($"Lock '{Name}' variables (convenience):");
                if (Locked != null)
                {
                    try
                    {
                        var dv = await session.ReadValueAsync(Locked.NodeId, System.Threading.CancellationToken.None);
                        Console.WriteLine($"  Locked = {dv?.Value} (status={dv?.StatusCode})");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  Locked read failed: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine("  Locked = <not discovered>");
                }

                if (LockingUser != null)
                {
                    try
                    {
                        var dv = await session.ReadValueAsync(LockingUser.NodeId, System.Threading.CancellationToken.None);
                        Console.WriteLine($"  LockingUser = {dv?.Value} (status={dv?.StatusCode})");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  LockingUser read failed: {ex.Message}");
                    }
                }
                if (LockingClient != null)
                {
                    try
                    {
                        var dv = await session.ReadValueAsync(LockingClient.NodeId, System.Threading.CancellationToken.None);
                        Console.WriteLine($"  LockingClient = {dv?.Value} (status={dv?.StatusCode})");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  LockingClient read failed: {ex.Message}");
                    }
                }
                if (RemainingLockTime != null)
                {
                    try
                    {
                        var dv = await session.ReadValueAsync(RemainingLockTime.NodeId, System.Threading.CancellationToken.None);
                        Console.WriteLine($"  RemainingLockTime = {dv?.Value} (status={dv?.StatusCode})");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  RemainingLockTime read failed: {ex.Message}");
                    }
                }
                if (CurrentState != null)
                {
                    try
                    {
                        var dv = await session.ReadValueAsync(CurrentState.NodeId, System.Threading.CancellationToken.None);
                        Console.WriteLine($"  CurrentState = {dv?.Value} (status={dv?.StatusCode})");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  CurrentState read failed: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Warn($"RemoteLock.PrintVariablesAsync failed: {ex.Message}");
            }
        }

        public Task<bool> HasMethodAsync(Session session, string methodName, int maxDepth = 3)
        {
            if (session == null) return Task.FromResult(false);
            try
            {
                var found = Methods.Keys.Any(k => !string.IsNullOrEmpty(k) && k.IndexOf(methodName, StringComparison.OrdinalIgnoreCase) >= 0);
                return Task.FromResult(found);
            }
            catch (Exception ex)
            {
                UAClient.Common.Log.Debug($"RemoteLock.HasMethodAsync: search failed: {ex.Message}");
            }
            return Task.FromResult(false);
        }

        // Note: runtime recursive browse helper removed. Discovery should populate Variables/Methods/Descriptor.
    }
}
