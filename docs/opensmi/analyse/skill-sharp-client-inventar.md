# Skill-Sharp-Client (SSC) inventory, as input for the OpenSMI C# client migration

Note: this is an analysis snapshot of the old Skill-Sharp-Client at `e57b3c0`, taken on 2026-10-29; that code now lives in `legacy/`. It is not updated and does not describe the new `OpenSmi.Client`.

Source: `scratchpad/src/ssc` (shallow clone of `github.com/BBlumhofer/Skill-Sharp-Client`, `main` @ `e57b3c0` "minor", 2026-02-13; only one commit is visible because the clone is shallow).
All paths below are relative to the repo root. `file:a-b` means a line range. "IMAPRO" means `C:\Users\BenjaminBlumhofer\Repos\DissCode\IMAPRO`.

## 1. Project and solution structure

| Item | Value | Source |
|---|---|---|
| Solution | `cs_client.slnx` (new XML solution format) with 2 projects: `UAClient/UAClient.csproj` and `UAClient.Tests/UAClient.Tests.csproj`. `Tools/RemoteInspector` is not in the solution. A second `UAClient.Tests/UAClient.Tests.slnx` contains only the test project. | `cs_client.slnx:1-4`, `UAClient.Tests/UAClient.Tests.slnx` |
| Library project | `UAClient/UAClient.csproj`: `OutputType=Library`, `TargetFramework=net10.0`, `Nullable=enable`, `ImplicitUsings=enable` | `UAClient/UAClient.csproj:4-8` |
| Assembly / PackageId | `AssemblyName=SkillSharp.Client`, `RootNamespace=SkillSharp.Client`, `PackageId=SkillSharp.Client`, `Version=1.0.0`, Description "OPC UA Client Library for Skill-based Manufacturing Systems" | `UAClient.csproj:9-13` |
| Odd build props | `GenerateRuntimeConfigurationFiles=true`, `CopyLocalLockFileAssemblies=true` (exe-style settings on a library) | `UAClient.csproj:15-16` |
| NuGet deps (library) | `OPCFoundation.NetStandard.Opc.Ua.Client` 1.5.377.22, `OPCFoundation.NetStandard.Opc.Ua.Core` 1.5.377.22 (nothing else; no `Microsoft.Extensions.Logging`) | `UAClient.csproj:20-21` |
| Test project | `net10.0`, `IsPackable=false`, `Microsoft.NET.Test.Sdk` 18.0.1, `xunit` 2.8.1, `xunit.runner.visualstudio` 2.8.0, ProjectReference `..\UAClient\UAClient.csproj` | `UAClient.Tests/UAClient.Tests.csproj` |
| Tool | `Tools/RemoteInspector/RemoteInspector.csproj`: Exe, net10.0, ProjectReference `..\..\UAClient\UAClient.csproj`; reads `config.json` | `Tools/RemoteInspector/*` |
| C# namespaces | `UAClient.Client` (all `Client/*.cs`), `UAClient.Common` (`Common/*.cs`), `UAClient` (`Program.cs`), `UAClient.Tests`. They do not match the `RootNamespace`/assembly name `SkillSharp.Client`. | e.g. `Client/RemoteServer.cs:10`, `Common/Enums.cs:1`, `Program.cs:10` |
| Size | Library: 6,933 LoC (Client 6,458 + Common 93 + Program 401). Tests: 1,182 LoC. Tool: 246 LoC. | `wc -l` |

### Build/test status (run on a clean copy in `scratchpad/ssc-build`, bin/obj/.vs removed)

- **Library builds**: `dotnet build UAClient/UAClient.csproj --no-incremental` -> success, **0 errors, 54 warnings**: CS8604 x14, CS8602 x12, CS8600 x10 (nullability), CS0618 x7 (obsolete sync `SessionClient.Read(...)` and `Session.Close(...)`), CS0649 x3 / CS0414 x3 / CS0169 x2 (unused or never-assigned fields), CS8765, CS8618, CS0108 (`RemotePort.CurrentState` hides `RemoteComponent.CurrentState`).
- **Solution build fails offline here**: `NU1100 Microsoft.NET.Test.Sdk (>= 18.0.1)` cannot be resolved. This environment has *no NuGet sources configured* (`dotnet nuget list source` -> none), so only the local cache is available. xunit 2.8.1 and runner 2.8.0 were also missing; NU1603 resolved them to 2.9.2 and 2.8.2. This is an environment problem, not a code defect.
- **Tests**: with `Microsoft.NET.Test.Sdk` changed to the cached 17.14.1 *in the scratch copy only*: **18/18 passed in 280 ms**. 7 of the 18 are integration tests that `return` early unless `RUN_INTEGRATION_TESTS=1`, so they pass without running anything (see §5). No live OPC UA server was contacted.
- Test-project analyzer warnings: CA2021 at `FullIntegrationTests.cs:86,194` and `StoreSkillFinalResultTests.cs:152`. `MethodSet.Values.OfType<RemoteSkill>()` is always empty because `MethodSet` is `Dictionary<string, RemoteMethod>` and `RemoteMethod`/`RemoteSkill` are sibling types, so those fallback lookups are dead code. There are also 4x xUnit2020 warnings.

## 2. Per-class inventory

Threading conventions used across the library (applies everywhere unless noted):
- Everything is `async Task`. There is no `CancellationToken` parameter on any public API; every SDK call passes `CancellationToken.None`.
- Pervasive `try { ... } catch { }` with silent swallowing: hundreds of empty catches in total.
- The public maps are plain `Dictionary<,>` exposed as `IDictionary<,>`. Some are written from parallel `Task.Run` workers, sometimes under ad-hoc locks (`lock(dictLock)`, `lock(nameMapping)`) and sometimes without any lock.
- Logging goes to the static `UAClient.Common.Log`, i.e. `Console.WriteLine`. There is no `ILogger`.

### 2.1 `UaClient` (`Client/UaClient.cs`, 439 lines)
**Purpose:** a thin wrapper around an OPC UA `Session`. It handles application config, endpoint selection, identity, session creation, and basic read/write/call.

| Member | Signature | Line |
|---|---|---|
| ctor | `UaClient(string url, string username = "", string password = "")` | 122 |
| ConnectAsync | `Task ConnectAsync(uint sessionTimeout = 60000)`. Returns early if already connected. Builds an `ApplicationConfiguration` in code, discovers endpoints, picks the highest SecurityLevel, and on failure falls back to SecurityPolicy None. | 129-244 |
| DisconnectAsync | `Task DisconnectAsync()` | 344-358 |
| Session | `Session? Session { get; }` (concrete `Session`, not `ISession`) | 360 |
| Configuration | `ApplicationConfiguration? Configuration { get; }` | 362 |
| ReplaceSession | `void ReplaceSession(Session newSession)`. Used by the reconnect handler; closes and disposes the old session. | 368-389 |
| ReadNodeAsync | `Task<object?> ReadNodeAsync(string nodeId)` | 391-397 |
| WriteNodeAsync | `Task WriteNodeAsync(string nodeId, object value)`. Throws `ServiceResultException` on a Bad status. | 399-415 |
| CallMethodAsync | `Task<object?> CallMethodAsync(string objectNodeId, string methodNodeId, params object[] inputs)` | 417-432 |
| Dispose | `void Dispose()` | 434-437 |

Threading: a `static ConcurrentDictionary<Session, KeepAliveState>` (line 18) shared across all instances, plus an `async` lambda KeepAlive handler (45-105).

Bugs and smells:
- **`CallMethodAsync` always returns `null` and swallows every exception** (`UaClient.cs:422-431`). Output arguments are discarded and failures are invisible.
- `Dispose()` does sync-over-async with `DisconnectAsync().Wait()` (436).
- Security config is hard-coded: `AutoAcceptUntrustedCertificates = true` and `AddAppCertToTrustedStore = true` (140-141). Cert stores sit under `CertificateStores/...` *relative to the CWD* (142-155). `ApplicationName = "UAClient"` is a constant (136). Because lock ownership is matched against this name, every instance of the library looks like the same client (see `RemoteModule.LockAsync`).
- Sessions are created by reflection over `DefaultSessionFactory`/`SessionFactory.CreateAsync`, with argument defaults guessed per parameter type (`CreateSessionCompatAsync`, 246-342). Every `bool` argument is set to `false` and every `string` to `"UAClientSession"`. This is fragile across SDK versions.
- A default `Subscription` with a 1000 ms publishing interval is created on every new session (322-328), and the SubscriptionManager also creates its own.
- An unconditional `Task.Delay(200)` runs after session creation (337).
- Smaller issues: the indentation of `ConnectAsync`/`CreateSessionCompatAsync` is broken (207-342), and `TransportQuotas.OperationTimeout=15000` is hard-coded.

### 2.2 `RemoteServer` (`Client/RemoteServer.cs`, 629 lines)
**Purpose:** the root object. It connects, builds the namespace map, discovers modules ("machines") and components, sets up subscriptions, and runs reconnect and auto-recovery.

Also defined in this file: `enum RemoteServerStatus { Disconnected, Connecting, Browsing, Connected }` (12-18), and `interface IRemoteServerSubscriber { OnConnectionLost(); OnConnectionEstablished(); OnServerTimeUpdate(DateTime); OnStatusChange(RemoteServerStatus); }` (20-26).

| Member | Signature | Line |
|---|---|---|
| ctor | `RemoteServer(UaClient client)`. Creates a `SubscriptionManager`. | 46-50 |
| SubscriptionManager | `SubscriptionManager SubscriptionManager { get; }` | 53 |
| Components | `IReadOnlyDictionary<string, RemoteComponent> Components` (key `"Module:Component"`) | 54 |
| Modules | `IReadOnlyDictionary<string, RemoteModule> Modules` (key = DisplayName) | 55 |
| NamespaceMap | `IReadOnlyDictionary<string,int> NamespaceMap` | 56 |
| Status | `RemoteServerStatus Status { get; private set; }` | 57 |
| AddSubscriber / RemoveSubscriber | `void AddSubscriber(IRemoteServerSubscriber)` / `void RemoveSubscriber(...)` | 59-60 |
| ConnectAsync | `Task ConnectAsync(double timeoutSeconds = 10, double reconnectTryDelay = 5, int? maxRetries = 3)` | 73-129 |
| Dispose | `void Dispose()` | 621-626 |

Private pipeline: `IterateMachinesAsync` (131-271), `DiscoverComponentsAsync` (273-331), `AutoReconnectLoop` (333-422), `RunPostReconnectRecoveryAsync` (424-468), `EnableSdkReconnect`/`DisableSdkReconnect`/`Session_KeepAlive`/`OnReconnectComplete` (470-580), `SetupAllSubscriptionsAsync` (583-619).

Threading:
- Module setup runs in parallel through a `SemaphoreSlim(6)` (180-270). Core subscriptions also use `SemaphoreSlim(6)` (585-618).
- A background `Task.Run(AutoReconnectLoop)` polls every 0.5 s (128, 420), and the **SDK `SessionReconnectHandler`** runs as a second reconnect path (491-494, 532). The two are gated by `IsSdkReconnectActive` (62-65).
- `lock(_keepAliveSync)` guards only the keep-alive session swap (474, 501).
- `_subscribers` is a plain `List` that is iterated from background threads (70, 351, 364).

Bugs and smells:
- **`timeoutSeconds` is passed as the *session timeout*** (`_client.ConnectAsync((uint)(timeoutSeconds*1000))`, 76). It is not a connect timeout, so the default 10 s produces a 10 s session timeout.
- `maxRetries` is never used (73). `_firstConnection` is set but never read (39, 118).
- **Two concurrent reconnect mechanisms**: the polling loop calls `_client.ConnectAsync()` and re-browses (355-415), while the SDK handler swaps the session (546-580). Both then run the full re-discovery plus `EnableAutoRecoveryAsync` plus `EnsureRecoveryAsync` on every module (391-403, 445-457).
- **Re-discovery after reconnect creates new `RemoteModule` objects** (`IterateMachinesAsync` -> `_modules[name] = new RemoteModule(...)`, 233-234). Callers that hold the old instances (IMAPRO did) keep stale objects. The old modules' auto-recovery handlers are still re-bound by `RebindSubscriptionsAsync`, so recovery can fire twice.
- The server-time subscription on node `i=2258` has an empty handler, and `OnServerTimeUpdate` is never invoked (108-116, 24).
- Module detection is heuristic. A node becomes a "module" if its TypeDefinition BrowseName contains `"Machine"` (226), and if neither the Machinery `Machines` folder nor SFM `ModuleSet` exists, any Objects child whose DisplayName contains "Module" or "Machine" is used as the module set (159-169).
- `DiscoverComponentsAsync` treats **module `Methods` entries** as component candidates (281-329). The "Methods" map actually holds skills and methods (see `RemoteModule`), so its contents are mixed.
- `Dispose` does not await the loop task, does not dispose `_client`, and calls the sync-over-async `SubscriptionManager.Dispose()`.
- The obsolete sync `session.Read(...)` is used (216).

### 2.3 `RemoteModule` (`Client/RemoteModule.cs`, 2,127 lines, a god class)
**Purpose:** one machine/module. It handles discovery of everything under the module (lock, ports, storages, components, skills, monitoring) and module lifecycle (lock, couple, startup skill, readiness, auto-relock, auto-recovery), plus SmartFactory resource resolution and neighbor extraction.

| Member | Signature | Line |
|---|---|---|
| AllowLockFallback | `bool AllowLockFallback { get; set; }`. Never read by the library; only `Program.cs` sets it. | 14 |
| Name / BaseNodeId | `string Name`, `NodeId BaseNodeId` | 16-17 |
| Methods | `IDictionary<string, BaseRemoteCallable> Methods`. A mixed bag: `name` -> `RemoteMethod` and `"skill:"+name` -> `RemoteSkill` for the same node. | 18 |
| Lock | `RemoteLock? Lock { get; private set; }` | 19 |
| IsLockedByUs | `bool IsLockedByUs { get; private set; }` (`lock(this)`) | 21 |
| Ports | `IDictionary<string, RemotePort> Ports` | 23 |
| Neighbors | `List<string> Neighbors { get; private set; }` | 24 |
| Storages | `IDictionary<string, RemoteStorage> Storages` | 31 |
| Components | `IDictionary<string, RemoteComponent> Components` (ports are inserted here too) | 32 |
| SkillSet | `IDictionary<string, RemoteSkill> SkillSet` | 33 |
| Monitoring | `IDictionary<string, RemoteVariable> Monitoring` | 34 |
| ctor | `RemoteModule(string name, NodeId baseNodeId, UaClient client, RemoteServer remoteServer)` | 39-45 |
| IsReady | `bool IsReady { get; private set; }` (`lock(this)`) | 48-53 |
| GetClosedPortsPartnerRfidTags | `IDictionary<string,string> GetClosedPortsPartnerRfidTags()` | 59-80 |
| GetNeighborIds | `IReadOnlyList<string> GetNeighborIds()` | 126-133 |
| EnableAutoRelockAsync / DisableAutoRelockAsync | `Task EnableAutoRelockAsync()` / `Task DisableAutoRelockAsync()` | 136-190 / 261-278 |
| DiscoverMethodsAsync | `Task DiscoverMethodsAsync()`. The main discovery routine. | 710-1021 |
| SetupSubscriptionsAsync | `Task SetupSubscriptionsAsync(SubscriptionManager? subscriptionManager = null, bool createSubscriptions = true)` | 1023-1104 |
| ResolveResourceNodeIdsAsync | `Task ResolveResourceNodeIdsAsync()` | 1112-1302 |
| SubscribeCoreAsync | `Task SubscribeCoreAsync(SubscriptionManager)` | 1305-1363 |
| CoupleAsync | `Task CoupleAsync(TimeSpan? timeout = null)`. Couples the first port only. | 1368-1389 |
| LockAsync | `Task<bool?> LockAsync(Session session)` | 1391-1538 |
| UnlockAsync | `Task<bool?> UnlockAsync(Session session)` | 1540-1564 |
| StartAsync | `Task StartAsync(bool reset = true, TimeSpan? timeout = null, bool couple = true)`. Starts the "Startup" skill. | 1569-1621 |
| EvaluateReadyAsync | `Task EvaluateReadyAsync()`. Ready = locked && first port coupled && startup skill Running. | 1624-1676 |
| MakeReadyAsync | `Task<bool> MakeReadyAsync(TimeSpan? timeout = null)` | 1679-1712 |
| EnableAutoRecoveryAsync / DisableAutoRecoveryAsync / EnsureRecoveryAsync | `Task EnableAutoRecoveryAsync()`, `Task DisableAutoRecoveryAsync()`, `Task EnsureRecoveryAsync(string reason = "ManualEnsureRecovery")` | 1770-1860 / 1862-1885 / 1892-1906 |

Private helpers: `ExtractNeighborIdFromPartnerRfidTag` (82-101), `RefreshNeighborsFromPorts` (103-124), `OnLockStateChangedAsync` (192-259), `GetTypeDefinitionAsync` (280-300), `GetBrowseNameAsync` (302-318), `BrowseAndLogNodeRecursive` (322-417, unused), `CreateRemoteObjectFromTypeDefinitionAsync` (419-708), `IsTypeOrSubtypeOfAsync` (1714-1751), `OnRecoveryLockChangedAsync` (1908-1966), `OnRecoveryStartupChangedAsync` (1968-2000), `TriggerRecoveryAsync` (2002-2125).

Threading:
- **`lock(this)`** is used at 21, 50-51, 119 and 129.
- `DiscoverMethodsAsync` processes each child in `Task.Run` and writes to the public dictionaries under a local `dictLock` (718, 801-847, 885, 932, 958, 970, 1008).
- Monitored-item callbacks fire and forget with `_ = Task.Run(async () => ...)` (150, 156, 168, 174, 180, 1794, 1801, 1810, 1836).
- `Interlocked.CompareExchange` on `_recoveryInProgress` prevents concurrent recoveries (2005).

Bugs and smells:
- `DiscoverMethodsAsync` claims to be BFS, but **children are never enqueued** (queue only contains `BaseNodeId`, 717-720), so discovery goes exactly one level deep, plus special-case descents into `MachineComponents` and `SkillSet`.
- The module's `SkillSet` is filled only when a direct child `SkillSet` exists (900-952). Skills found by the "name contains Skill" heuristic go only into `Methods` (955-963). But `StartAsync`, `EvaluateReadyAsync` and the recovery code look up the startup skill in `Methods.Values.OfType<RemoteSkill>()` (1580-1582, 1663, 1823-1825, 1974-1976, 2086-2088), while IMAPRO looked in `SkillSet`. The two can disagree.
- Every skill is registered twice (`Methods[name] = new RemoteMethod(sameNode)` and `Methods["skill:"+name] = skill`, 936-937, 960-961, 997-998, 837-838), so `SetupSubscriptionsAsync` subscribes the same nodes twice (1041-1044).
- `SetupSubscriptionsAsync(..., createSubscriptions:false)` (as called by `RemoteServer`, 251) still creates subscriptions for all methods and skills, because it calls `m.SetupSubscriptionsAsync(subscriptionManager, true)` (1043).
- **Lock ownership check is unsound.** `OwnedByUs` treats an owner string as ours if it contains our ApplicationName (always `"UAClient"`), our hostname, machine name or any local IP, or `localhost`/`127.0.0.1`/`::1` (1411-1477). If the owner cannot be read, it **assumes the lock is ours** (1521-1529). Any two SSC clients on the same host, or any two SSC instances at all, see each other's lock as their own.
- `StartAsync` uses `dynamic` casts (1607, 1612-1613) to call methods that exist statically. Its `reset` flag only changes a log line (1598-1601).
- `MakeReadyAsync` may create a throw-away `SubscriptionManager` (1685).
- Dead code: the `_autoReady*` and `_makeReadyInProgress` fields (1754-1759), `AllowLockFallback`, `BrowseAndLogNodeRecursive`, and `type_def` (458).
- Two near-duplicate subscription mechanisms exist, auto-relock (136-259) and auto-recovery (1770-2125). Both subscribe to lock state and start relocking. Enabling both doubles the reactions.
- The obsolete sync `session.Read` is used (309, 342, 1205, 1272). There is copy-paste duplication for CarrierType/ProductType (1156-1289).
- `Neighbors` is a public `List<string>` with a private setter but can be mutated freely. The neighbor ID comes from parsing an RFID string (82-101).

### 2.4 `BaseRemoteCallable` (`Client/BaseRemoteCallable.cs`, 206 lines; file is not UTF-8, it contains a cp1252 dash)
**Purpose:** abstract base for `RemoteMethod` and `RemoteSkill`. It holds the `FinalResultData`, `Monitoring` and `ParameterSet` variable maps and implements `IDataChangeHandler`.

| Member | Signature | Line |
|---|---|---|
| Name / BaseNodeId | `string Name`, `NodeId BaseNodeId` | 13-14 |
| RemoteServerClient | `protected UaClient RemoteServerClient` | 15 |
| FinalResultData / Monitoring / ParameterSet | `IDictionary<string, RemoteVariable>` (case-insensitive) | 18-20 |
| ctor | `protected BaseRemoteCallable(string name, NodeId baseNodeId, UaClient client)` | 32-37 |
| SetupSubscriptionsAsync | `virtual Task SetupSubscriptionsAsync(SubscriptionManager? subscriptionManager, bool createSubscriptions = true)`. Browses direct children named FinalResultData, Monitoring or ParameterSet. | 40-128 |
| AddSubscriber | `void AddSubscriber(Action<RemoteVariable> callback)`. Called only on FinalResultData changes. | 154-157 |
| SubscribeCoreAsync | `virtual Task SubscribeCoreAsync(SubscriptionManager)` | 160-176 |
| DataChangeNotification | `virtual void DataChangeNotification(NodeId, DataValue, MonitoredItemNotificationEventArgs)` | 178-204 |

Threading: `lock` on the dictionaries themselves (78, 93, 97, 116). Initial reads run in parallel with `SemaphoreSlim(10)` (131-152).
Smells: creates its own `SubscriptionManager` when it is passed `null` (43-51). `_subscribers` is an unsynchronized `List`.

### 2.5 `RemoteMethod` (`Client/RemoteMethod.cs`, 76 lines)
**Purpose:** a callable OPC UA method.

| Member | Signature | Line |
|---|---|---|
| ctor | `RemoteMethod(string name, NodeId baseNodeId, UaClient client)` | 11-13 |
| CallAsync | `Task<IDictionary<string, object?>> CallAsync(params object[] inputs)` | 15-74 |

Bugs: `CallAsync` browses `HasComponent` children of BaseNodeId to find a method named "Call" or the first one (19-49). Most `RemoteMethod`s are constructed with the *method node itself* as BaseNodeId (`RemoteModule.cs:487, 553, 1008`, `RemoteComponent.cs:355`). For those, the browse finds nothing and the call throws "No method found to call". It also passes BaseNodeId as the `ObjectId`. **Output arguments are discarded**: it returns a re-read of `FinalResultData` instead (61-73).

### 2.6 `RemoteSkill` (`Client/RemoteSkill.cs`, 1,508 lines)
**Purpose:** a skill instance. It discovers the SkillExecution/StateMachine nodes, tracks state (cached via subscription, with a read fallback), handles parameters, final results and monitoring, and implements Start/Halt/Reset/Execute with heuristics.

| Member | Signature | Line |
|---|---|---|
| StateMachineNode / CurrentStateNode / StartMethodNode / ResetMethodNode / HaltMethodNode | `NodeId?` getters | 29-33 |
| CurrentStateVariable | `RemoteVariable?` (from `Monitoring["CurrentState"]`) | 36-43 |
| CurrentState | `SkillStates CurrentState` (the cached value, defaults to `Halted`) | 46 |
| ctor | `RemoteSkill(string name, NodeId baseNodeId, UaClient client, RemoteServer remoteServer, bool? isFiniteHint = null)` | 48-52 |
| FindMethodNodeRecursive | `Task<NodeId?> FindMethodNodeRecursive(Session session, NodeId startNode, string methodName)` (public) | 54-132 |
| WriteParameterAsync | `Task WriteParameterAsync(string parameterName, object value)` | 134-196 |
| GetStateAsync | `Task<int?> GetStateAsync()` | 251-333 |
| IsFinite | `bool IsFinite { get; }` | 339-360 |
| SetupSubscriptionsAsync | `override Task SetupSubscriptionsAsync(SubscriptionManager, bool createSubscriptions = false)` | 444-567 |
| SubscribeCoreAsync | `override Task SubscribeCoreAsync(SubscriptionManager)` | 570-620 |
| AddStateSubscriber | `void AddStateSubscriber(Action<SkillStates> callback)` (no Remove) | 622-626 |
| DataChangeNotification | `override void DataChangeNotification(...)` | 636-668 |
| WaitForStateAsync | `Task<bool> WaitForStateAsync(SkillStates desired, TimeSpan timeout, bool preferSubscriptions = true)` | 784-808 |
| SetInputParametersAsync | `Task SetInputParametersAsync(IDictionary<string, object?>? parameters, bool skipNullValues = false)` | 810-824 |
| ReadInputParametersAsync | `Task<IDictionary<string, object?>> ReadInputParametersAsync(IEnumerable<string>? parameterNames = null, bool refresh = false)` | 826-850 |
| ReadMonitoringDataAsync | `Task<IDictionary<string, object?>> ReadMonitoringDataAsync(IEnumerable<string>? variableNames = null, bool refresh = false)` | 852-878 |
| StartAsync | `Task StartAsync(params object[] inputs)` | 880-951 |
| HaltAsync | `Task HaltAsync()` | 953-959 |
| ResetAsync | `Task ResetAsync()` | 961-967 |
| ExecuteAsync | `Task<IDictionary<string, object?>> ExecuteAsync(IDictionary<string, object?>? parameters = null, bool waitForCompletion = true, bool resetAfterCompletion = true, bool resetBeforeIfHalted = true, TimeSpan? timeout = null)` | 969-1208 |
| GetSuccessfulExecutionsCountAsync | `Task<long?> GetSuccessfulExecutionsCountAsync()` | 1210-1216 |
| ReadFinalResultDataAsync | `Task<IDictionary<string, object?>?> ReadFinalResultDataAsync(TimeSpan? timeout = null, long? expectedSuccessfulExecutions = null)` (polls every 100 ms) | 1221-1274 |
| ReadFinalResultDataSnapshotAsync | `Task<IDictionary<string, object?>> ReadFinalResultDataSnapshotAsync()` | 1280-1284 |

Threading:
- The cached `_cachedState` is written from SDK notification threads without synchronization (656). It is read everywhere (910, 1155, 1165) and is not `volatile`.
- **`_stateSubscribers` is a plain `List<Action<SkillStates>>`.** It is mutated by concurrent waiters (`AddStateSubscriber`, 625; `Remove`, 780) while `NotifyStateSubscribers` enumerates it on the notification thread (630-633). `ExecuteAsync` itself starts two waiters concurrently (1141-1142). A "collection was modified" exception is then swallowed by the `catch {}` in `DataChangeNotification` (661), so **state notifications can be lost**.
- `EnsureCoreSubscribedAsync` takes a lock that protects nothing: the flag is only set after the await (1467-1505), so concurrent callers subscribe twice.

Bugs and smells:
- **`_startMethodNode`, `_resetMethodNode` and `_haltMethodNode` are never assigned** (declared 18-20, no assignments; CS0649). Every Start, Halt or Reset call re-runs the method search (TranslateBrowsePaths, then a BFS of up to 1000 nodes, 54-132).
- **The `ObjectId` passed to `Call` is inconsistent.** Start uses `BaseNodeId` (893) and retries on the StateMachine node (935-950). `HaltAsync` and `ResetAsync` use `BaseNodeId` (958, 966), while `ExecuteAsync` resets on `_stateMachineNode ?? BaseNodeId` (1036, 1184). The method node itself is found under `StateMachine`. Per OPC UA, ObjectId must be the object owning the method, so some servers will return `BadMethodInvalid`.
- `StartAsync` busy-polls `_cachedState` at 20 Hz for 2 s (902-928) before returning, so it adds latency.
- `ExecuteAsync` **ignores the result of the completion wait** (1172-1173). On timeout it returns `FinalResultData` as if the skill had finished. Parameter-write failures are logged and the skill is still started (997-1008). A `null` parameter value is written as `""` (1002, 822).
- `WriteParameterAsync` throws in the ParameterSet path (145-154) but swallows non-range errors in the fallback path (192-195). A parameter that is not found only logs a warning and returns (161-165).
- There is no Suspend support, although `SkillStates.Suspended`/`Suspending` and `SkillNotSuspendableError` exist. `TryRecoverFromInvalidStateAsync` (1314-1362) performs Halt and Reset on its own.
- `IsFinite` falls back to "has FinalResultData" (357-358), which is a weak heuristic. `GetStateAsync` does up to four browse searches on a cache miss (275, 290, 294). `ReadHistoryAsync` and the other helpers are duplicated across classes (`GetTypeDefinitionAsync` and `GetBrowseNameAsync` are copy-pasted in `RemoteSkill`, 404-442, and `RemoteModule`, 280-318). The obsolete sync `session.Read` is used (433).

### 2.7 `RemoteComponent` (`Client/RemoteComponent.cs`, 428 lines)
**Purpose:** a generic sub-component, such as a Machinery `MachineComponents` entry or anything that has SkillSet/Monitoring/... children. It is recursive.
Also defined in this file: `interface IRemoteComponentSubscriber { void OnStateChange(string? newState); void OnNewMessage(RemoteComponent component, string text, int severity, string code); }` (12-16).

| Member | Signature | Line |
|---|---|---|
| Name / BaseNodeId | `string Name { get; protected set; }`, `NodeId BaseNodeId { get; protected set; }` | 20-21 |
| SkillSet | `IDictionary<string, RemoteSkill>` | 26 |
| FeasibilityCheckSet / PreconditionCheckSet | `IDictionary<string, RemoteSkill>`. Never populated. | 27-28 |
| Components | `IDictionary<string, RemoteComponent>` | 29 |
| MethodSet | `IDictionary<string, RemoteMethod>` | 30 |
| Attributes / Monitoring / ParameterSet | `IDictionary<string, RemoteVariable>` | 33-35 |
| CurrentState | `string? CurrentState` (from `Monitoring["CurrentState"]` or `["State"]`) | 45-59 |
| ctor | `RemoteComponent(string name, NodeId baseNodeId, UaClient client, RemoteServer remoteServer)` | 61-67 |
| Dto | `IDictionary<string, object?> Dto()` (snake_case keys, Python-style) | 69-84 |
| AddSubscriber | `void AddSubscriber(IRemoteComponentSubscriber s)` | 86-89 |
| DataChangeNotification | `void DataChangeNotification(...)` (not virtual) | 92-123 |
| EventNotification | `void EventNotification(MonitoredItem, MonitoredItemNotificationEventArgs)` | 126-137 |
| RegisterMonitoringNode | `void RegisterMonitoringNode(RemoteVariable rv)` (test helper) | 140-145 |
| SetupSubscriptionsAsync | `Task SetupSubscriptionsAsync(SubscriptionManager, bool createSubscriptions = false)` | 147-376 |
| SubscribeCoreAsync | `Task SubscribeCoreAsync(SubscriptionManager)` | 379-397 |
| GetChild / GetChildren | `object? GetChild(string name)`, `IEnumerable<(string name, string type)> GetChildren()` | 400-426 |

Threading: child processing runs in parallel with `SemaphoreSlim(8)` (162-375), and nested components use `Task.Run` (286-302). **The `SkillSet`, `Components`, `_monitoring_nodes`, `_parameter_nodes`, `_attribute_nodes` and `MethodSet` dictionaries are written from parallel tasks without locks** (199, 212, 228, 239, 298, 356).

Bugs:
- **The Attributes branch subscribes `_parameter_nodes` instead of `_attribute_nodes`** (`RemoteComponent.cs:243`).
- `OnStateChange` is fired for *any* monitoring variable change, not only for state changes (101-114).
- `EventNotification` passes `NotificationValue.ToString()` with severity 0 and an empty code (130-133), so event fields are never parsed.
- `Dto()` reports `"type" = "Generic"` (74).
- `SetupSubscriptionsAsync` is not virtual, and `RemotePort` overloads it with a different signature (`RemotePort.cs:286`).

### 2.8 `RemotePort : RemoteComponent` (`Client/RemotePort.cs`, 327 lines)
**Purpose:** a SmartFactory coupling port with RFID tags, magnets and a `CoupleSkill`.

| Member | Signature | Line |
|---|---|---|
| Module | `RemoteModule? Module { get; }` | 10 |
| ctor | `RemotePort(string name, NodeId baseNodeId, UaClient client, RemoteServer remoteServer, RemoteModule? module = null)` | 12-16 |
| CurrentState | `object? CurrentState` (hides the base member; CS0108) | 19-27 |
| ActivePort / OwnRfidTag / PartnerRfidTag / Closed / Type | `bool` (`Attributes["IsActive"]`), `string?` (`Attributes["OwnRfidTag"]`), `string?` (`Monitoring["PartnerRfidTag"]`), `bool` (`Monitoring["MagnetsClosed"]`), `"Port"` | 30-66 |
| IsCoupledAsync | `Task<bool> IsCoupledAsync(UaClient client)`. True if `CoupleSkill` is Running, else true if PartnerRfidTag is non-empty. | 68-106 |
| CoupleAsync | `Task CoupleAsync(UaClient client, TimeSpan? timeout = null)` | 108-249 |
| UncoupleAsync | `Task UncoupleAsync(UaClient client, TimeSpan? timeout = null)` | 251-283 |
| SetupSubscriptionsAsync | `Task SetupSubscriptionsAsync(SubscriptionManager? subscriptionManager)` (an overload, not an override) | 286-309 |
| Variables / NodeMap | aliases for `Attributes` / `_attribute_nodes` | 312-313 |
| Methods / MethodsNodeMap | `IDictionary<string, BaseRemoteCallable>`, `IDictionary<NodeId, BaseRemoteCallable>` | 316-317 |

Smells: `internal static RemoteModuleHelper.GetUaClientFromModule` reads `RemoteModule._client` by reflection (320-326). The `client` parameters are redundant because the base already has a client. `client.Session` is used without a null check (176, 181, 210, 217, 271, 275). The Reset/Start retry logic is duplicated from `RemoteSkill`.

### 2.9 `RemoteLock` (`Client/RemoteLock.cs`, 318 lines) and `RemoteLockDescriptor` (`Client/RemoteLockDescriptor.cs`, 20 lines)
**Purpose:** DI `LockingServicesType` access.

| Member | Signature | Line |
|---|---|---|
| ctor | `RemoteLock(string name, NodeId baseNodeId)` | 35-39 |
| Locked / LockingUser / LockingClient / RemainingLockTime / CurrentState | `RemoteVariable?` (get/set) | 17-21 |
| Variables / NodeMap | `IDictionary<string, RemoteVariable>`, `IDictionary<NodeId, RemoteVariable>` | 22-23 |
| Methods / MethodsNodeMap | `IDictionary<string, BaseRemoteCallable>`, `IDictionary<NodeId, BaseRemoteCallable>` | 25-26 |
| Descriptor | `RemoteLockDescriptor?` | 28 |
| InitLock / BreakLock / ExitLock / RenewLock | `BaseRemoteCallable?` | 31-34 |
| EnsureSubscriptionsAsync | `Task EnsureSubscriptionsAsync(SubscriptionManager?, UaClient? client = null)` | 41-63 |
| IsLockedAsync | `Task<bool?> IsLockedAsync(Session session)`. Throws if `Locked` is missing or not a bool. | 65-105 |
| GetLockOwnerAsync | `Task<string?> GetLockOwnerAsync(Session session)` (LockingClient, then LockingUser) | 108-145 |
| InitLockAsync | `Task<bool> InitLockAsync(Session session)` | 147-175 |
| ReleaseLockAsync | `Task<bool> ReleaseLockAsync(Session session)` | 177-201 |
| WaitForLockedAsync | `Task<bool> WaitForLockedAsync(Session session, TimeSpan timeout)` (polls every 500 ms) | 203-221 |
| PrintVariablesAsync | `Task PrintVariablesAsync(Session session)` (writes to the console) | 223-299 |
| HasMethodAsync | `Task<bool> HasMethodAsync(Session session, string methodName, int maxDepth = 3)` (sync, ignores maxDepth) | 301-314 |

`RemoteLockDescriptor`: `NodeId BaseNodeId; NodeId? InitLock, BreakLock, ExitLock, RenewLock, Locked, LockingUser, LockingClient, RemainingLockTime, CurrentState` (`RemoteLockDescriptor.cs:7-18`).

Bugs:
- **`InitLockAsync` iterates `{InitLock, RenewLock, ExitLock, AcquireLock, EnterLock, RequestLock, Lock, Init}` with substring matching and continues on failure** (152-172). If `InitLock` throws (for example because the lock is already held or arguments are missing), it goes on to `RenewLock` and then **`ExitLock`**. An "init" can therefore *release* the lock and still return `true`.
- **No input arguments are passed** (164). DI `InitLock` expects a `Context` (String) input and returns `InitLockStatus`, and the status output is never checked. To verify against the target server.
- Substring matching on `"Lock"` or `"Init"` can hit unrelated methods.
- Methods whose name contains "Skill" become `RemoteSkill`s inside the lock (`RemoteModule.cs:495-500`).

### 2.10 `RemoteStorage` (`Client/RemoteStorage.cs`, 154 lines) and `RemoteStorageSlot` (`Client/RemoteStorageSlot.cs`, 122 lines)
**Purpose:** a SmartFactory storage with slots that carry carrier and product IDs and types.

`RemoteStorage`: `RemoteStorage(string name, NodeId baseNodeId)` (15); `Name`, `BaseNodeId`, `Variables`, `NodeMap`, `Slots` (9-13); `object? CurrentState` (22-36); `Task SetupSubscriptionsAsync(SubscriptionManager?, UaClient? client = null)` (38-65); `Task<(bool found, RemoteStorageSlot? slot)> IsInStorageAsync(UaClient? client = null, string? productId = null, NodeId? productType = null, string? carrierId = null, NodeId? carrierType = null)` (69-152).

`RemoteStorageSlot`: `RemoteStorageSlot(string name, NodeId baseNodeId)` (13); `Name`, `BaseNodeId`, `Variables`, `NodeMap` (8-11); `Type => "StorageSlot"` (20); `string? CarrierId` (22-29, `CarrierID`); `object? CarrierType` (31-42); `string? ProductId` (44-47, `ProductID`); `bool? IsSlotEmpty` (49-62); `bool? IsCarrierEmpty` (64-77); `object? ProductType` (79-90); `NodeId? ResolvedCarrierTypeNodeId`, `ResolvedProductTypeNodeId`, `string? ResolvedCarrierTypeName`, `ResolvedProductTypeName` (93-98); `string CarrierTypeDisplay()` (100-106); `string ProductTypeDisplay()` (108-114); `object? State` (117-120).

Smells: `Variables` in both classes are case-sensitive `Dictionary`s (`RemoteStorage.cs:11`, `RemoteStorageSlot.cs:11`), while most other maps are case-insensitive. `IsInStorageAsync` uses `client.Session` without a null check (91).

### 2.11 `RemoteVariable` and `RemoteVariableDto` / `TimestampedValue` (`Client/RemoteVariable.cs`, 126 lines)

| Member | Signature | Line |
|---|---|---|
| RemoteVariableDto | `Name, NodeId (string), Value, IsWritable, UaDataType, ValidValues` | 8-16 |
| ctor | `RemoteVariable(string name, NodeId nodeId)` | 38-41 |
| props | `string Name {get;set;}`, `NodeId NodeId {get;set;}`, `object? Value {get; private set;}`, `DateTime? Timestamp`, `bool IsWritable`, `string UaDataType = "UNKNOWN"`, `int UaValueRank`, `IDictionary<object,string>? ValidValues`, `Variant? LastVariant`, `IDictionary<string,RemoteVariable> Children`, `EUInformation? Unit`, `Range? Range` | 20-31 |
| SetClient | `void SetClient(UaClient client)` | 36 |
| SetupSubscriptionAsync | `Task SetupSubscriptionAsync(SubscriptionManager)` | 43-54 |
| DataChangeNotification | `void DataChangeNotification(...)` | 57-65 |
| UpdateFromDataValue | `void UpdateFromDataValue(DataValue dv)` | 67-73 |
| ToDto | `RemoteVariableDto ToDto()` | 75-86 |
| ReadHistoryAsync | `Task<IList<TimestampedValue>> ReadHistoryAsync(Session, DateTime?, DateTime?, uint)`. A stub that always returns an empty list. | 88-93 |
| WriteValue | `void WriteValue(object value)`. Sync over async (`GetAwaiter().GetResult()`). | 95-110 |
| WriteValueAsync | `Task WriteValueAsync(object value)` | 112-118 |
| TimestampedValue | `object? Value; DateTime? Timestamp` | 121-125 |

Smells: `UaDataType`, `UaValueRank`, `ValidValues`, `Unit`, `Range` and `Children` are never populated anywhere. `IsWritable` is set by the folder (true for ParameterSet), not from AccessLevel. `Value` and `Timestamp` are not synchronized. Writes are done as `new Variant(value)` with no DataType coercion (`UaClient.cs:403-408`), so writing an `int` to a `Double` node fails with `BadTypeMismatch`.

### 2.12 `RemoteVariableCollector` (`Client/RemoteVariableCollector.cs`, 152 lines)
`static Task AddVariableNodesAsync(Session session, NodeId startNode, IDictionary<NodeId, RemoteVariable> nodeMapping, IDictionary<string, RemoteVariable> nameMapping, bool isWritable, int maxNodes = 5000)` (12-150). It runs a recursive BFS over HierarchicalReferences, creates a `RemoteVariable` for every Variable node and does an initial read for each (`SemaphoreSlim(20)`, `Task.Run` per variable). It excludes EURange, EngineeringUnits and similar names (65-76).
Smells: variables are keyed by DisplayName (53). When nested variables have the same name, only the first is kept (113-116). Properties of variables (child variables) are flattened into the same map. There is one Read RPC per variable and no batching.

### 2.13 `SubscriptionManager` and `IDataChangeHandler` (`Client/SubscriptionManager.cs`, 385 lines)
`interface IDataChangeHandler { void DataChangeNotification(NodeId nodeId, DataValue value, MonitoredItemNotificationEventArgs args); }` (11-14)

| Member | Signature | Line |
|---|---|---|
| ctor | `SubscriptionManager(UaClient client)`. Adds an *uncreated* default subscription to the session. | 32-48 |
| Dispose / DisposeAsync | sync-over-async `Dispose()`; `ValueTask DisposeAsync()` deletes the subscriptions | 50-62 |
| CreateSubscriptionAsync | `Task<Subscription> CreateSubscriptionAsync(double publishingInterval = 1000)` | 64-74 |
| AddMonitoredItemAsync | `Task<MonitoredItem> AddMonitoredItemAsync(NodeId nodeId, Action<MonitoredItem, MonitoredItemNotificationEventArgs> handler, Subscription? subscription = null, uint samplingInterval = 0)` | 93-114 |
| AddEventMonitoredItemAsync | `Task<MonitoredItem> AddEventMonitoredItemAsync(NodeId nodeId, Action<...> handler, Subscription? subscription = null)` (no EventFilter, QueueSize 0) | 116-136 |
| SubscribeDataChangeAsync | `Task SubscribeDataChangeAsync(IDataChangeHandler handler, IEnumerable<NodeId> nodes)` | 138-176 |
| RemoveMonitoredItemAsync | `Task RemoveMonitoredItemAsync(MonitoredItem item)` | 296-308 |
| RebindSubscriptionsAsync | `Task RebindSubscriptionsAsync()`. Deletes all subscriptions and recreates the items on the current session. | 312-383 |

Threading: `ConcurrentDictionary` for `_monitored`, `_handlers` and `_rawHandlers` (25-29). `_subscriptions` is a plain `List` (24). `EnsureDefaultSubscriptionAsync` is unsynchronized (76-91), so parallel callers (the module setup runs 6 in parallel) can race `CreateAsync`. A `SemaphoreSlim _reconnectLock` guards only Rebind (19, 314).

Bugs:
- **`SubscribeDataChangeAsync` ignores the notified value.** On every notification it issues a **new `ReadValueAsync` against the `session` captured at subscribe time** and passes that to the handler (153-164). This adds one RPC per change, can reorder values, and reads from a dead session after a session swap (the exception is swallowed, so the update is lost silently).
- **There is no de-duplication.** Every `SubscribeCoreAsync`/`SetupSubscriptionsAsync` pass adds new monitored items for the same nodes, and those passes run at connect, after reconnect, and lazily per skill.
- `RebindSubscriptionsAsync` recreates event items as plain value items without a handler (`AttributeId` is captured but `AddMonitoredItemAsync` always uses `Attributes.Value`; event items have no `_rawHandlers` entry, 357-369), so event monitoring is lost after a reconnect.
- `AddEventMonitoredItemAsync` uses `ApplyChangesAsync` without retry and stores no handler for rebind (130-135). `_lastReconnectAttempt` and `_reconnectCooldown` are unused (20-21).

### 2.14 `SessionBrowseCache` (`Client/SessionBrowseCache.cs`, 185 lines)
`static bool TryGet(Session, NodeId start, string[] path, out NodeId? nodeId)` (27-37); `static void Store(Session, NodeId, string[], NodeId)` (39-43); `static Task<NodeId?> TranslatePathAsync(Session session, NodeId start, string[] path)` (50-183).
Static, unbounded `ConcurrentDictionary<string,string>` keyed by `sessionId|start|path` (18), with no eviction.
**Likely ineffective:** the path elements are built with `new QualifiedName(p)` and therefore namespace index 0 (66, 133). TranslateBrowsePaths compares namespace-qualified names, so paths through vendor or companion-spec nodes (SkillSet, SkillExecution, StateMachine, Start/Reset/Halt in the skill namespace) will not resolve, and every caller falls back to a slow BFS. Only ns0 names such as `CurrentState` can hit. This is inferred from the code and should be verified against a live server. The SDK response is unpacked through reflection (86-103, 147-164).

### 2.15 `UaHelpers` (`Client/UaHelpers.cs`, 66 lines)
`static NodeId ToNodeId(ExpandedNodeId expanded, Session session)` (9-64). It **throws `NotSupportedException` for absolute ExpandedNodeIds** (63). Callers mostly wrap it in `catch {}`, so remote-server references are silently dropped.

### 2.16 `LockTest` (`Client/LockTest.cs`, 75 lines)
`static Task<int> Run(string url, string username, string password, string moduleName = "CA-Module")` (10-73). This is a console demo that lives inside the library and writes to `Console`.

### 2.17 `Common/*` (93 lines)
- `Enums.cs` (17 lines): `enum SkillStates { Halted=11, Ready=12, Running=13, Suspended=14, Completed=15, Starting=16, Halting=17, Completing=18, Resetting=19, Suspending=20 }` (4-16). The comment says "translation of relevant enums from Python" (3).
- `Exceptions.cs` (19 lines): `PyUaAdapterError`, `SkillHaltedError`, `UnsupportedSkillNodeSetError`, `SkillNotSuspendableError`, and `PyUaRuntimeError(string msg, string errorCode)`. **None of them is ever thrown** (grep). The names are pyuaadapter leftovers.
- `Log.cs` (39 lines): a static console logger whose level comes from env `UACLIENT_LOG_LEVEL` (11). It uses the local timestamp (37).
- `NamespaceUris.cs` (18 lines): 12 constants (5-16). **Only `NS_MACHINERY_URI` and `NS_SFM_V3_URI` are used** (`RemoteServer.cs:149,153`).

### 2.18 `Program.cs` (401 lines) and `Tools/RemoteInspector/Program.cs` (246 lines)
- `UAClient/Program.cs` is a `static async Task<int> Main(string[] args)` **inside the library assembly** (34). Its modes are `--skilltest`, `--locktest`, `--autoready` and the default `isinstorage` (60-73, 151-155). It contains hard-coded defaults `opc.tcp://localhost:4842` and **`orchestrator`/`orchestrator`** credentials (58, 71-85), the module name `"CA-Module"` (162), and `Components["Robot"].SkillSet["MoveToJointPosition"]` (267-268). It sets global `TaskScheduler.UnobservedTaskException` and `AppDomain.UnhandledException` handlers (37-54) that would affect any host that loads it.
- `RemoteInspector`: a console app that dumps a module's ports, neighbors, storages/slots, components and skills from `config.json` (18-210). It is not in `cs_client.slnx`.

## 3. Classification: (G) generic, maps onto OpenSMI concepts, vs. (S) SmartFactory/IMAPRO-specific

OpenSMI concept columns: Srv = server/connection, Mach = machine/module, Comp = component, Skill, Meth = method, Var = variable, Lock, Sub = subscription. "G*" means the feature is generic but the current implementation relies on SmartFactory heuristics and should be reimplemented rather than ported.

### 3.1 Connection / server
| Feature | Class | OpenSMI | Location |
|---|---|---|---|
| Endpoint discovery and selection, None fallback, user/anon identity | G | Srv | `UaClient.cs:129-244` |
| Session creation via reflection | G* (replace with `DefaultSessionFactory`/`ISessionFactory` directly) | Srv | `UaClient.cs:246-342` |
| Read/Write/Call by string NodeId | G | Var/Meth | `UaClient.cs:391-432` |
| Namespace map (URI -> index) | G | Srv | `RemoteServer.cs:80-90` |
| Status enum and subscriber events | G | Srv | `RemoteServer.cs:12-26, 59-71` |
| SDK reconnect handler and KeepAlive | G | Srv | `RemoteServer.cs:470-580`, `UaClient.cs:28-114` |
| Polling AutoReconnectLoop (second mechanism) | G* (drop; keep one) | Srv | `RemoteServer.cs:333-422` |
| Post-reconnect: re-discover plus auto-recovery of modules | S (recovery semantics) | Mach | `RemoteServer.cs:391-403, 445-457` |
| Server time subscription (i=2258) | G (currently a no-op) | Sub | `RemoteServer.cs:108-116` |

### 3.2 Discovery
| Feature | Class | OpenSMI | Location |
|---|---|---|---|
| Find `Machines` (OPC 40001 Machinery) under Objects | G | Mach | `RemoteServer.cs:149` |
| Find `ModuleSet` in `http://smartfactory.de/UA/SFM/` | S | Mach | `RemoteServer.cs:153` |
| DisplayName contains "Module"/"Machine" fallback; TypeDefinition name contains "Machine" | G* (heuristic) | Mach | `RemoteServer.cs:159-169, 224-230` |
| Global component index from module `Methods` heuristic | G* | Comp | `RemoteServer.cs:273-331` |
| Type-based object factory (`IsTypeOrSubtypeOfAsync` walking HasSubtype) | G | Comp/Skill/Lock | `RemoteModule.cs:419-708, 1714-1751` |
| Machinery/IA/DI type list -> `RemoteComponent` | G (lists SF types too: `StorageSlotType`, `GripperType`, `ShuttleType`, `ConveyingAreaType` = S) | Comp | `RemoteModule.cs:422-450` |
| `MachineComponentsType` container expansion | G (Machinery) | Comp | `RemoteModule.cs:776-857` |
| "Has SkillSet/Monitoring/MethodSet/Attributes/Components/Resources child" => component | G* (heuristic; `Resources` = S) | Comp | `RemoteModule.cs:859-897`, `RemoteServer.cs:304-309` |
| `SkillSet` folder / `SkillSetType` -> skills | G | Skill | `RemoteModule.cs:900-952`, `RemoteComponent.cs:179-207` |
| "name contains Skill" => RemoteSkill | G* (heuristic) | Skill | `RemoteModule.cs:955-963, 495-500, 556-561` |
| `FiniteSkillType`/`ContinuousSkillType` | G | Skill | `RemoteModule.cs:451-455, 702-706`; `RemoteSkill.cs:479-493` |
| Monitoring container (`MonitoringType` or name) | G | Var | `RemoteModule.cs:763-773` |
| Component: Monitoring/ParameterSet/Attributes/Identification/Components/MethodSet/Notification.Messages | G | Comp/Var/Meth/Sub | `RemoteComponent.cs:147-376` |
| `FeasibilityCheckSet` / `PreconditionCheckSet` (declared only) | G (OpenSMI-relevant, unimplemented) | Skill | `RemoteComponent.cs:27-28` |
| Recursive variable collection | G | Var | `RemoteVariableCollector.cs:12-150` |
| TranslateBrowsePaths cache | G (needs namespace-aware names) | Srv | `SessionBrowseCache.cs` |
| ExpandedNodeId -> NodeId | G | Srv | `UaHelpers.cs:9-64` |

### 3.3 Skills
| Feature | Class | OpenSMI | Location |
|---|---|---|---|
| Locate `SkillExecution/StateMachine/CurrentState`, `AvailableStates` | G | Skill | `RemoteSkill.cs:455-477` |
| State parse (numeric 11-20 or name), cached state, state subscribers | G | Skill/Sub | `RemoteSkill.cs:251-333, 622-697`; `Enums.cs:4-16` |
| Start/Halt/Reset method lookup and call | G | Skill/Meth | `RemoteSkill.cs:54-132, 880-967` |
| WaitForState (subscription then poll) | G | Skill | `RemoteSkill.cs:755-808` |
| Parameter write/read (ParameterSet) | G | Skill/Var | `RemoteSkill.cs:134-196, 810-850` |
| Monitoring read | G | Skill/Var | `RemoteSkill.cs:852-878` |
| FinalResultData snapshot / read | G | Skill/Var | `RemoteSkill.cs:1221-1312` |
| `SuccessfulExecutionsCount` extraction / wait | G* (a FinalResultData naming convention; OpenSMI-like but SF-originated) | Skill | `RemoteSkill.cs:1210-1216, 1393-1403` |
| `ExecuteAsync` orchestration (reset-before, start, wait, reset-after, BadInvalidState recovery) | G (orchestration policy; IMAPRO did not use it) | Skill | `RemoteSkill.cs:969-1208, 1314-1362` |
| `IsFinite` heuristic | G* | Skill | `RemoteSkill.cs:339-360` |

### 3.4 Methods / variables / subscriptions
| Feature | Class | OpenSMI | Location |
|---|---|---|---|
| `RemoteMethod.CallAsync` | G (broken, see §2.5) | Meth | `RemoteMethod.cs:15-74` |
| `MethodSet` on components | G | Meth | `RemoteComponent.cs:339-363` |
| `RemoteVariable` value cache, write, DTO | G | Var | `RemoteVariable.cs` |
| `ReadHistoryAsync` stub | G (unimplemented) | Var | `RemoteVariable.cs:88-93` |
| Subscription manager: data change, raw handlers, events, rebind | G | Sub | `SubscriptionManager.cs` |
| Component events (`Notification/Messages`) -> `OnNewMessage` | G (OpenSMI-like "messages") | Comp/Sub | `RemoteComponent.cs:126-137, 309-338` |

### 3.5 Locking
| Feature | Class | OpenSMI | Location |
|---|---|---|---|
| `LockingServicesType` (OPC 10000-100 DI) detection, descriptor, Init/Exit/Renew/Break, Locked/LockingClient/LockingUser/RemainingLockTime | G | Lock | `RemoteModule.cs:460-526`; `RemoteLock.cs`; `RemoteLockDescriptor.cs` |
| Module `LockAsync`/`UnlockAsync`, `IsLockedByUs` | G (but ownership matching is broken, §2.3) | Lock/Mach | `RemoteModule.cs:1391-1564` |
| Auto-relock on lock loss | G* (policy) | Lock/Sub | `RemoteModule.cs:136-278` |

### 3.6 SmartFactory / IMAPRO-specific (S)
| Feature | Location |
|---|---|
| `RemotePort` (whole class): IsActive, OwnRfidTag, PartnerRfidTag, MagnetsClosed, `CoupleSkill`, Couple/Uncouple | `RemotePort.cs:1-327`; type detection `PortType` `RemoteModule.cs:528-569` |
| Module `Ports` map, `CoupleAsync` (first port), `IsReady` coupling criterion | `RemoteModule.cs:23, 1368-1389, 1643-1657` |
| Neighbors from RFID tag parsing (`..._<id>_...Gate...`) | `RemoteModule.cs:24, 82-133, 1020` |
| `GetClosedPortsPartnerRfidTags` | `RemoteModule.cs:59-80` |
| `RemoteStorage`/`RemoteStorageSlot` (whole classes), `StorageType`/`StorageSlotType`, slots under `Components` | `RemoteStorage.cs`, `RemoteStorageSlot.cs`; `RemoteModule.cs:571-694` |
| Slot vars `CarrierID`, `CarrierType`, `ProductID`, `ProductType`, `IsSlotEmpty`, `IsCarrierEmpty` | `RemoteStorageSlot.cs:22-90` |
| `IsInStorageAsync` | `RemoteStorage.cs:69-152` |
| `ResolveResourceNodeIdsAsync` (dotted paths such as `CA-Module.Resources.Cab_A_Blue...`) | `RemoteModule.cs:1106-1302` |
| "Startup" skill convention (name contains "Startup"), `StartAsync`, `EvaluateReadyAsync`, `MakeReadyAsync` | `RemoteModule.cs:1566-1712` |
| Auto-recovery (lock lost or stolen, startup halted: relock, couple all ports, restart startup) | `RemoteModule.cs:1753-2125` |
| `NS_SKILL_SET_URI`, `NS_MACHINE_SET_URI`, `NS_ECM_SFKL_URI`, `NS_SFM_V3_URI` (`http://smartfactory.de/UA/...`) | `NamespaceUris.cs:9-10, 15-16` |
| `Program.cs` scenarios (`CA-Module`, `Robot`, `MoveToJointPosition`), `LockTest`, `RemoteInspector` | `Program.cs`, `LockTest.cs`, `Tools/RemoteInspector/Program.cs` |
| `Exceptions.cs` pyuaadapter names | `Exceptions.cs:5-17` (neutral but unused) |

## 4. Information-model assumptions

**Namespace URIs** (`Common/NamespaceUris.cs:5-16`): DI `http://opcfoundation.org/UA/DI/`, RSL `.../RSL/`, Machinery `.../Machinery/`, SkillSet `http://smartfactory.de/UA/SkillSet`, MachineSet `http://smartfactory.de/UA/MachineSet`, Robotics, IA, MachineTool, LaserSystems, ECM, ECM-SFKL `http://smartfactory.de/UA/EnergyConsumptionManagement`, SFM v3 `http://smartfactory.de/UA/SFM/`. Only Machinery and SFM v3 are used (`RemoteServer.cs:149,153`). **Everything else matches on unqualified names, ignoring namespaces**: DisplayName or BrowseName `.Name`, often by substring, case-insensitive.

**Entry point** (`RemoteServer.cs:140-176`): browse `ObjectsFolder` (HierarchicalReferences; Object, Variable and Method classes), then:
1. `Machinery:Machines` (149), else
2. `SFM:ModuleSet` (153), else
3. the first child whose DisplayName contains "Module" or "Machine" (162-168).

The children of that folder (default browse) are module candidates. A candidate is kept iff its TypeDefinition BrowseName contains `"Machine"` (196-230). The module key is the DisplayName (189, 234).

**Module children** (`RemoteModule.DiscoverMethodsAsync`, one level, 720-1018), in decision order:
1. TypeDef contains `MonitoringType` or BrowseName = `Monitoring`: variables go to `Monitoring` (763-773).
2. TypeDef contains `MachineComponentsType`: each child is passed to the type factory (776-857).
3. The child has a sub-child named `SkillSet|Monitoring|MethodSet|Attributes|Components|Resources`: it becomes a `RemoteComponent` (859-897).
4. BrowseName or DisplayName = `SkillSet`, or TypeDef contains `SkillSetType`: each child except `SkillSetType` goes through the factory into `SkillSet` and `Methods` (900-952).
5. The name contains `Skill`: it becomes a `RemoteSkill` in `Methods` (955-963).
6. The type factory (967-1001): a `LockingServicesType` subtype gives `RemoteLock` (460). `PortType` gives `RemotePort` (528). `StorageType` gives `RemoteStorage` (571). The Machinery/IA set gives `RemoteComponent` (422-450, 696). `FiniteSkillType|ContinuousSkillType` gives `RemoteSkill` (451-455, 702).
7. Otherwise a Method node becomes a `RemoteMethod` (1006-1009).

Type matching compares the BrowseName.Name of the TypeDefinition and its supertypes (`IsTypeOrSubtypeOfAsync`, 1714-1751), ignoring namespaces.

**Component children** (`RemoteComponent.SetupSubscriptionsAsync`, 179-363): `SkillSet` (children whose BrowseName ends in `Type` or contains `SkillExecution` are skipped, 197), `Monitoring`, `ParameterSet`, `Attributes` or `Identification`, `Components` (HasComponent plus Organizes, 246-308), `Notification/Messages` (event notifier, 309-338) and `MethodSet` (339-363).

**Skill structure** (`RemoteSkill.SetupSubscriptionsAsync`, 455-564): `<skill>/.../SkillExecution/.../StateMachine/{CurrentState, AvailableStates}`, each found by a BFS on DisplayName/BrowseName. The containers `Monitoring`, `ParameterSet` and `FinalResultData` are looked up under the skill, then under SkillExecution, then under StateMachine (537-564). The base class looks only at direct children (`BaseRemoteCallable.cs:74-124`).

**Skill state IDs**: `SkillStates` Halted=11, Ready=12, Running=13, Suspended=14, Completed=15, Starting=16, Halting=17, Completing=18, Resetting=19, Suspending=20 (`Enums.cs:6-15`). CurrentState is accepted as an int/uint/short/ushort number or as a (Localized)Text name, with a `(...)` suffix stripped (`RemoteSkill.cs:258-268, 670-697`). Fallbacks read `StateNumber` or `State` under CurrentState or StateMachine, then the DisplayName of the CurrentState node (285-316). Transition states: Starting, Halting, Completing, Resetting, Suspending (699-712). Reset candidates: Halted, Completed, Suspended (714-725). Finite: TypeDefinition `FiniteSkillType`, else AvailableStates contains `Completed`/`Completing`, else FinalResultData is non-empty (339-360, 488-489).

**Skill method names**: `Start`, `Halt`, `Reset`. They are found via the paths `[m]`, `[StateMachine, m]`, `[SkillExecution, StateMachine, m]`, then a HasComponent BFS (`RemoteSkill.cs:59-65, 68-131`). The call ObjectId is the skill node (Start/Halt/Reset) or the StateMachine node (Execute's reset, Start retry) (893, 940, 958, 966, 1036, 1044, 1184-1191). Start takes `params object[] inputs`; Halt and Reset take no inputs. **There is no Suspend or Resume.** Parameters are found via ParameterSet, then `[name]`, `[ParameterSet,name]`, `[Monitoring,name]`, `[SkillExecution,StateMachine,name]`, then a BFS (`RemoteSkill.cs:198-249`).

**Startup and coupling conventions**: the startup skill is the first `RemoteSkill` in module `Methods` whose name contains `Startup` (`RemoteModule.cs:1580-1582`). The couple skill is named exactly `CoupleSkill`: port SkillSet, then port Methods, then module Methods (`RemotePort.cs:74, 86, 115-139`). Coupled = CoupleSkill Running, else PartnerRfidTag is non-empty (`RemotePort.cs:68-106`).

**Lock**: TypeDefinition `LockingServicesType` or a subtype (`RemoteModule.cs:460`). Methods are matched by substring: `InitLock`, `BreakLock`, `ExitLock`, `RenewLock` (491-494). `InitLockAsync` candidates, in order: `InitLock, RenewLock, ExitLock, AcquireLock, EnterLock, RequestLock, Lock, Init` (`RemoteLock.cs:152`). Release candidates: `ReleaseLock, ExitLock, Unlock, Release, LeaveLock` (`RemoteLock.cs:180`). The methods are called with no inputs on the lock object (`RemoteLock.cs:164, 191`). Variables are matched by substring: `Locked`, `LockingUser`, `LockingClient`, `RemainingLockTime`, and `CurrentState` **or any name containing "State"** (`RemoteModule.cs:512-516`). `Locked` must be a Boolean, otherwise an exception is thrown (`RemoteLock.cs:76-77, 93-94`). The owner is LockingClient, then LockingUser (`RemoteLock.cs:114-138`).

**Ports**: TypeDefinition `PortType` (`RemoteModule.cs:528`). Attributes `IsActive` and `OwnRfidTag`; Monitoring `PartnerRfidTag` and `MagnetsClosed` (`RemotePort.cs:34-61`). The neighbor ID is the segment between the last two underscores before `Gate` in PartnerRfidTag (`RemoteModule.cs:89-100`).

**Storage**: TypeDefinition `StorageType` (`RemoteModule.cs:571`). A slot is identified by BrowseName containing `StorageSlot` or by `StorageSlotType`, either directly or under `Components` (597-608, 630-688). Slot variables are listed in §3.6. Resource resolution of `CarrierType`/`ProductType` values parses a NodeId string or a dotted browse path from slot, storage, module or Objects (`RemoteModule.cs:1130-1154`).

**Other fixed IDs**: ServerStatus.CurrentTime is `i=2258` (`RemoteServer.cs:110`). `SuccessfulExecutionsCount` is found by key substring in FinalResultData (`RemoteSkill.cs:1399`).

## 5. Tests (`UAClient.Tests`, xUnit, `DisableTestParallelization=true` in `AssemblyInfo.cs:3`)

| File (lines) | Covers | Live server? |
|---|---|---|
| `BaseRemoteCallableTests.cs` (68) | `DataChangeNotification` updates FinalResultData and notifies the subscriber; a Monitoring update does not notify (30-66). Uses a fake `UaClient("opc.tcp://localhost:4840")` that is never connected. | No |
| `RemoteComponentTests.cs` (44) | `RegisterMonitoringNode` plus `DataChangeNotification` causes `IRemoteComponentSubscriber.OnStateChange` (24-42) | No |
| `RemoteLockTests.cs` (27) | `HasMethodAsync(null, ...)` returns false; ctor properties (11-25) | No |
| `RemoteVariableTests.cs` (30) | `UpdateFromDataValue` sets value and timestamp; `WriteValue` without a client throws (9-28) | No |
| `SubscriptionManagerTests.cs` (33) | `AddMonitoredItemAsync`/`SubscribeDataChangeAsync` throw without a session (12-26) | No |
| `UaClientTests.cs` (24) | ctor; `ReadNodeAsync` without a session throws (10-22) | No |
| `IntegrationTests.cs` (101) | Connect and discover (asserts `Modules.Count >= 0`, a tautology, line 46); lock and start the `CA-Module` startup skill (54-99). Line 81 bug: `GetEnumerator().Current` without `MoveNext()` is always null and throws. | Yes, gated by `RUN_INTEGRATION_TESTS=1`, otherwise passes vacuously |
| `FullIntegrationTests.cs` (249) | `Module_FullLifecycle_EndToEnd`: ports > 0, skill present, lock true, couple, `Robot.MoveToJointPosition` execute with random joint angles, FinalResultData (25-153). `Skills_ShouldExpose_State_Finals_Monitoring_Parameters_And_TypeHints`: state is in the enum, Startup and Couple are continuous, Move is finite (155-247). | Yes (same gate) |
| `ModuleLifecycleTests.cs` (161) | `MakeReadyAsync` gives `IsReady` (24-62); `IsInStorageAsync` finds a known ProductID (64-159) | Yes (same gate) |
| `StoreSkillFinalResultTests.cs` (442) | Lock, couple, start Startup, run a "Store"-named skill twice, and check that `SuccessfulExecutionsCount` increases by >= 2 (96-440) | Yes (same gate) |

Live config for every integration test: env `UA_TEST_SERVER` (default **`opc.tcp://172.24.100.85:4892`**), `UA_TEST_USER` and `UA_TEST_PASS` (default **`orchestrator`/`orchestrator`**). They assume module `CA-Module` (fallback: the first module), component `Robot`, skills `MoveToJointPosition`, `CoupleSkill`, `*Startup*` and `*Store*`. See `IntegrationTests.cs:34-36`, `FullIntegrationTests.cs:30-32,43`, `ModuleLifecycleTests.cs:29-31`, `StoreSkillFinalResultTests.cs:101-103`. These tests target the SmartFactory-KL hardware or simulation and are not generic.

Coverage gap: nothing tests discovery, skill state handling, ExecuteAsync, locking or subscriptions without hardware. There are no mocks or fakes of `ISession`. The committed TRX files (2025-11-30) contain one test each, and the message uses MSTest's `Assert.AreEqual` text, so the tests were migrated from MSTest at some point.

## 6. Repo hygiene

1. **Build output is committed**: 190 tracked files under `bin/` or `obj/` (`UAClient/bin`, `UAClient/obj`, `UAClient.Tests/bin` with 98 files including localized TestPlatform resource DLLs, `UAClient.Tests/obj`, and `Tools/RemoteInspector/bin|obj` including `RemoteInspector.exe`). There is **no `.gitignore`**.
2. **IDE state is committed**: 13 files under `UAClient.Tests/.vs/`, including `.suo`, `DocumentLayout*.json`, `TestStore/*.testlog`, `.dtbcache.v2` and Copilot indices `CopilotIndices/18.3.180.61135/CodeChunks.db` and `SemanticSymbols.db`.
3. **Test results are committed**: `UAClient.Tests/TestResults/BenjaminBlumhofer_DESKTOP-73VA9N6_2025-11-30_09_43_{38,58}.trx`. They leak the user and host name.
4. **Credentials and an internal IP are committed**: `Tools/RemoteInspector/config.json` (`opc.tcp://192.168.178.30:4892`, `visitor`/`visitor`). There are hard-coded defaults `orchestrator`/`orchestrator` and `172.24.100.85:4892` in `Program.cs:71-85` and all integration tests.
5. **Demo code is in the library**: `UAClient/Program.cs` (a `Main` in an `OutputType=Library` project, plus global exception handlers) and `Client/LockTest.cs`. `Client/RemoteLock.PrintVariablesAsync` writes to `Console`. The logger is console-only (`Common/Log.cs`).
6. **Naming is inconsistent**: the repo is "Skill-Sharp-Client", the folder `UAClient`, the assembly and PackageId `SkillSharp.Client`, and the namespaces `UAClient.*`. The exception names are pyuaadapter-style (`PyUaAdapterError`, ...).
7. **Two solution files** (`cs_client.slnx`, `UAClient.Tests/UAClient.Tests.slnx`). `Tools/RemoteInspector` is in neither.
8. **The README is effectively empty**: `README.md` is a UTF-16 LE file containing only the title "Skill-Sharp-Client". There is no LICENSE, no CHANGELOG and no CI config.
9. **Encoding**: `Client/BaseRemoteCallable.cs` is not valid UTF-8 (cp1252 dash at line 42). `RemotePort.cs:142` contains a literal U+FFFD.
10. **Pinned versions are not resolvable from a cache-only feed**: test SDK 18.0.1 and xunit 2.8.1 / runner 2.8.0 (NU1100 / NU1603 in §1).
11. **Code-health signals**: 54 compiler warnings, including obsolete sync SDK calls; unused fields and parameters (`maxRetries`, `_firstConnection`, `_autoReady*`, `AllowLockFallback`, `_start/_reset/_haltMethodNode`); two 1.5k-2.1k-line god classes; and hundreds of empty `catch {}` blocks.
12. The history in this clone is shallow (only `e57b3c0` is visible). The real history is on GitHub.

## 7. How IMAPRO used SSC (for API-compatibility decisions)

Link: `MAS-BT.csproj` had `<ProjectReference Include="../Skill-Sharp-Client/UAClient/UAClient.csproj" />`. It was removed in IMAPRO `b764bd6` ("Fix build-blocking project references") because the folder was missing in that checkout. In `fc21f7a` ("Replace OPC UA hardware execution with a simulated hardware layer"), 32 files were switched to `MAS_BT.Hardware.Simulation`, a same-shaped drop-in (`IMAPRO/Hardware/Simulation/*.cs`, 1,015 lines). That simulation layer is effectively a **record of the SSC subset IMAPRO consumed**. Today IMAPRO has no SSC dependency; only comments and docs mention it (`Nodes/Locking/LockResourceNode.cs:10`, `CLAUDE.md`, `.github/copilot-instructions.md`, `docs/TestCoverageProgress.md`).

Consumers at `fc21f7a^` were 31 files in `Nodes/**`, `Services/{RemoteServerMqttNotifier,SkillPreconditionChecker,StorageMqttNotifier}.cs` and `Examples/ModuleInitializationTestRunner.cs`.

Member usage at `fc21f7a^` (count of matching lines in those files):

| Type | Members used | Examples (IMAPRO path:line at `fc21f7a^`) |
|---|---|---|
| `UaClient` | `new UaClient(endpoint, user, pass)` (1), `.Session` (11; `Session.Connected`) | `Nodes/Configuration/ConnectToModuleNode.cs:77, 93-102` |
| `RemoteServer` | `new RemoteServer(client)`, `ConnectAsync(timeoutSeconds)` (2), `Modules` (42), `Status` + `RemoteServerStatus.Connected`, `SubscriptionManager`, `AddSubscriber` | `ConnectToModuleNode.cs:36, 49, 80, 84`; `Services/StorageMqttNotifier.cs:62` |
| `IRemoteServerSubscriber` | implemented: `OnConnectionLost`, `OnStatusChange(RemoteServerStatus)` (plus the rest of the interface) | `Services/RemoteServerMqttNotifier.cs:13, 37` |
| `SubscriptionManager` | `AddMonitoredItemAsync(nodeId, handler)` (raw handler; comment notes it "does not update RemoteVariable.Value automatically") | `Services/StorageMqttNotifier.cs:182-190` |
| `RemoteModule` | `Name`, `SkillSet` (36: `.Count`, `.Values`, `.TryGetValue`, `.Keys`), `Methods` (2), `Ports` (1), `Storages` (15), `Lock` (3), `LockAsync(session)` (4), `IsLockedByUs` (11), `EnableAutoRecoveryAsync()` (1), `GetNeighborIds()` (1) | `Nodes/Locking/LockResourceNode.cs:88`, `Nodes/Recovery/EnsureModuleLockedNode.cs:49-87`, `ConnectToModuleNode.cs:120`, `Nodes/ModuleHolon/ReadNeighborsFromRemoteNode.cs:43` |
| `RemoteLock` | `GetLockOwnerAsync(session)` | `LockResourceNode.cs:146-149` |
| `RemotePort` | `IsCoupledAsync(client)`, `CoupleAsync(client, timeout)`, `SkillSet.Keys`, `Methods.Values.OfType<RemoteSkill>()`, `Module.Methods`, `Module.SkillSet` | `Nodes/Configuration/EnsurePortsCoupledNode.cs:59-154` |
| `RemoteSkill` | `Name` (86 `.Name` hits overall), `CurrentState` (31, cached `SkillStates`), `GetStateAsync()` (9), `StartAsync()` (2), `HaltAsync()` (1), `ResetAsync()` (5), `WaitForStateAsync(state, timeout)` (4), `SetInputParametersAsync(dict)` (1), `IsFinite` (2), `GetSuccessfulExecutionsCountAsync()` (1), `ReadFinalResultDataAsync(window, expectedCount)` (2), `ReadFinalResultDataSnapshotAsync()` (2) | `Nodes/SkillControl/ExecuteSkillNode.cs:176-578`, `Nodes/Recovery/EnsureStartupRunningNode.cs:48-115`, `Nodes/Recovery/HaltAllSkillsNode.cs:40-96` |
| `RemoteStorage` / `RemoteStorageSlot` | `Slots` (21), `Variables` (`TryGetValue("IsSlotEmpty"/"CarrierID"/"ProductID"/"ProductType"/"CarrierType")`), `IsInStorageAsync(...)`, `CarrierId`, `ProductId` (26), `IsSlotEmpty` (7), `CarrierTypeDisplay()`, `ProductTypeDisplay()` | `Nodes/Monitoring/ReadStorageNode.cs:94-106`, `Services/SkillPreconditionChecker.cs:255`, `Services/StorageMqttNotifier.cs:423-434` |
| `RemoteVariable` | `Value`, `NodeId`, `UpdateFromDataValue(dv)` | `StorageMqttNotifier.cs:425, 434` |
| `UAClient.Common.SkillStates` | 40 references (Halted/Ready/Running/Completed checks) | throughout `Nodes/**` |
| `Opc.Ua` types | `ServiceResultException`, `StatusCode.IsBad`, `NodeId`, `DataValue`, `MonitoredItemNotificationEventArgs` (pattern matching only) | `ExecuteSkillNode.cs:581`; `IMAPRO/Hardware/Simulation/OpcTypes.cs` |

Observations relevant to the migration:
- IMAPRO **did not use `RemoteSkill.ExecuteAsync`**. `ExecuteSkillNode` orchestrated GetState, Reset, WaitForState(Ready), SetInputParameters, Start, WaitForState(Completed or Halted per `IsFinite`) and ReadFinalResultData itself (`ExecuteSkillNode.cs:212-360`).
- IMAPRO looked up the startup skill in `module.SkillSet` by `Name` containing "Startup" (`CheckStartupSkillStatusNode.cs:52-53`, `EnsureStartupRunningNode.cs:48-49`, `SkillPreconditionChecker.cs:154`). SSC's own recovery looks in `module.Methods` instead.
- IMAPRO's `IAsyncCallable` (`Nodes/SkillControl/IAsyncCallable.cs`) was an IMAPRO-local interface that SSC types never implemented, so the `skill is IAsyncCallable` branches in `AbortSkillNode.cs:53` and `RetrySkillNode.cs:64,74` were dead code against SSC.
- None of these were used by IMAPRO: `RemoteComponent`, `RemoteMethod.CallAsync`, `MakeReadyAsync`, `IsReady`, `UnlockAsync`, `ReadMonitoringDataAsync`, `ReadInputParametersAsync`, `Components`, `NamespaceMap`, or events/messages.
- Minimal surface a compatible replacement needs (from the simulation shim): `UaClient(endpoint,user,pass)` + `Session.Connected`; `RemoteServer(client).ConnectAsync`, `.Modules`, `.Status`, `.SubscriptionManager.AddMonitoredItemAsync`, `.AddSubscriber`; `RemoteModule.{Name, SkillSet, Ports, Storages, Methods, Lock.GetLockOwnerAsync, LockAsync, IsLockedByUs, EnableAutoRecoveryAsync, GetNeighborIds}`; `RemoteSkill.{Name, CurrentState, IsFinite, GetStateAsync, StartAsync, ResetAsync, HaltAsync, WaitForStateAsync, SetInputParametersAsync, GetSuccessfulExecutionsCountAsync, ReadFinalResultDataAsync, ReadFinalResultDataSnapshotAsync}`; `RemotePort.{IsCoupledAsync, CoupleAsync, SkillSet, Methods, Module}`; `RemoteStorage.{Slots, Variables, IsInStorageAsync}`; `RemoteStorageSlot.{Variables, CarrierId, ProductId, IsSlotEmpty, *TypeDisplay}`; `SkillStates`; `IRemoteServerSubscriber`; `RemoteServerStatus`.
