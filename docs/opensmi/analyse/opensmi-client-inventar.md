# OpenSMI Python Client: API and behavior inventory (basis for a C# port)

Path prefixes used in citations:

- `osc/` = `scratchpad/src/osc` (client). Client modules are cited as `client/<file>` = `osc/src/opensmi/client/<file>`.
- `ocore/` = `scratchpad/src/ocore`. Core modules are cited as `core/<file>` = `ocore/src/opensmi/core/<file>`.
- `osrv/` = `scratchpad/src/osrv`. Server modules are cited as `server/<file>` = `osrv/src/opensmi/server/<file>`. Nodeset XMLs are cited as `nodesets/<file>` = `osrv/src/opensmi/server/nodesets/<file>`.

Snapshot: osc HEAD `e8740c2` (2026-10-23), ocore HEAD `68ef623` (2026-10-23), osrv HEAD `29e4d4a` (2026-10-29). None of the clones has git tags, so the runtime version (setuptools-scm) cannot be read from them.

---

## 1. Package overview

| Item | Value | Source |
|---|---|---|
| Distribution name | `OpenSMI-Client`, import namespace `opensmi.client` (PEP 420 namespace package `opensmi`) | `osc/pyproject.toml:9`, `:77` (`namespaces = true`) |
| Purpose | "Asynchronous Python client for interacting with Smart Machine Interface-compliant OPC UA servers" | `osc/pyproject.toml:12` |
| Version | dynamic via setuptools-scm; `__version__ = metadata.version("OpenSMI-Client")` | `osc/pyproject.toml:5,10`; `client/__init__.py:23` |
| Python | `requires-python = ">=3.11"`, classifiers 3.11 to 3.14 | `osc/pyproject.toml:29-32,41` |
| Runtime deps | `asyncua~=2.1.0`, `opensmi-core~=0.5.0` | `osc/pyproject.toml:43-46` |
| Transitive (core) | `asyncua>=2.0.1,<3`, `structlog>=26`, `typing-extensions>=4.16.0` | `ocore/pyproject.toml:39-43` |
| Extras | `rich` (rich, colorama on win32), `json` (orjson) | `osc/pyproject.toml:51-58` |
| License | MIT (library); examples CC0-1.0 | `osc/pyproject.toml:17-18`; `osc/README.md:86-91`; `osc/LICENSES/` |
| Authors | Carsten Harms (DFKI), Simon Jungbluth (SmartFactory) | `osc/pyproject.toml:13-15` |
| Repo | github.com/SmartFactory-KL/opensmi-client | `osc/pyproject.toml:49` |
| Status | "Development Status :: 4 - Beta" classifier, but README calls the API "stable", following semver | `osc/pyproject.toml:22`; `osc/README.md:80-84` |

Public exports from `opensmi.client`: `AutoLock, BaseRemoteComponent, NotificationEvent, RemoteComponent, RemoteLock, RemoteMachine, RemoteMethod, RemoteResource, RemoteServer, RemoteServerStatus, RemoteSkill, RemoteUser, RemoteVariable` (`client/__init__.py:9-21`). `RemoteCallable`, `RemoteVariableContainer` (and `Attributes`, `Identification`, `Monitoring`, `ParameterSet`, `FinalResultData`), `RemoteUaObject`, the DTOs, `browse` helpers, `RemoteLockState`, `RemoteLockUaNodes`, `TimestampedValue` and `RemoteUserData` are importable only from their submodules.

Core exports used by the client: `Signal`, `SkillState`, `MachineryItemState`, `MachineryOperationMode`, `LifecycleState`, `LifecycleMixin`, `lifecycle`, `setup_logging`, `LogFormat`, `Unit` (`core/__init__.py:9-19`).

Programming model: everything is `async` (asyncio). Callbacks are async functions attached to `Signal` objects. Logging goes through structlog.

---

## 2. Classes

### 2.0 Cross-cutting base: `BaseUaObject` (core), `LifecycleMixin` (core), `RemoteUaObject` (client)

**`BaseUaObject`** (`core/base_ua_object.py:21-139`)
- ctor `(*, name: str|None=None, server=None, parent=None, **kwargs)`. `name` defaults to the class name. Raises `TypeError` for a non-str or empty name (`:42-46`).
- `ua_node` property (`:59-66`) raises `RuntimeError` if unset. The setter can be called **once only** (`RuntimeError` otherwise) and **registers the object in `server._ua_objects`**, keyed by NodeId (`:68-76`).
- `name` (`:78-81`), `path: PurePosixPath` = `parent.path / name`, or `/name` when there is no parent (`:91-97`). `full_name` is deprecated (`:83-89`).
- `server` property: resolved lazily from the parent chain and then cached (`:122-135`). `parent` getter/setter (`:104-113`). `logger` is a structlog BoundLogger (`:99-102`).

**`LifecycleMixin`** (`core/lifecycle_mixin.py:107-236`)
- `async init() -> Self`: idempotent, runs only from `NEW` state. Runs `@lifecycle` async-generator resources in topological order. On failure it rolls back the started resources in reverse order and re-raises; if the rollback also fails it raises a `BaseExceptionGroup` (`:128-173`).
- `async shutdown()`: idempotent, runs only when initialized. Raises `ExceptionGroup` on failures (`:187-204`).
- `is_initialized`, `lifecycle_state: LifecycleState`, `lifecycle_order` (`:222-236`). States are listed in `core/enums.py:8-24` (NEW, INITIALIZING, INITIALIZED, INITIALIZATION_FAILED, SHUTTING_DOWN, SHUTDOWN, SHUTDOWN_FAILED).
- Ordering cycles raise `CycleError` at class-definition time (`:114-126`).

**`RemoteUaObject[_ParentType]`** (`client/remote_ua_object.py:25-98`), base of all remote objects
- ctor `(*, name: str|LocalizedText, ua_node: Node|None, parent, server: RemoteServer, browse_rules=None, ua_type: str|None=None, **kw)`. A `LocalizedText` name is converted to its `.Text`. `ua_node` is set (and registered) only if it is truthy (`:48-49`). `ua_type` defaults to `"UNKNOWN"`. `browse_rules=None` becomes `(DEFAULT_BROWSE_RULE,)` (`:30-56`).
- `init_exceptions: list[Exception]`, marked "EXPERIMENTAL". Non-fatal child-browse failures are collected here instead of being raised (`:58-59`; see §8).
- `browse_rules` (`:61-64`). `parent` getter/setter; the setter asserts the value is a `RemoteUaObject` (`:80-88`).
- Lifecycle: a single `@lifecycle lifecycle_object` that calls the abstract `_init()` before the yield and `_shutdown()` (a no-op) after it (`:66-78`). So `await obj.init()` performs the browse.
- `async ua_read_type() -> str`: reads the BrowseName of the HasTypeDefinition target (`core/ua_node_util.py:131-146`) and caches it (`:90-93`). `ua_type` property (`:95-98`).
- **No async context manager** on any remote object except `RemoteServer`.

### 2.1 `RemoteServerStatus` (Enum, `client/remote_server.py:44-58`)
`DISCONNECTED="disconnected"`, `CONNECTING`, `BROWSING`, `CONNECTED`, `RECONNECTING`, `DISCONNECTING`.

### 2.2 `RemoteServer` (`client/remote_server.py:61-491`)
Bases: `BaseServer[RemoteUaObject]` and `UaDataChangeSubscriber`.

**Constructor** (`:78-166`). The constructor does not connect.

| Arg | Default | Meaning |
|---|---|---|
| `url` | required | e.g. `opc.tcp://localhost:4843/` |
| `username`, `password` | required (kw-only) | UserName identity token (`:122-125`) |
| `application_uri` | random `urn:uuid:<uuid4>` | client ApplicationUri, also used as the cert SAN (`:127-131`) |
| `timeout` | 10.0 s | asyncua request timeout |
| `watchdog_interval` | 1.0 s | liveness check (passed as asyncua's misspelled `watchdog_intervall`, `:116`) |
| `auto_reconnect` | True | asyncua auto-reconnect |
| `reconnect_max_delay` | 30.0 s | asyncua backoff cap |
| `reconnect_request_timeout` | 60.0 s | how long requests block while reconnecting |
| `security_policy` | `SecurityPolicyType.NoSecurity` | see §7 |
| `browse_rules` | `None` meaning browse everything; `()` meaning browse nothing | `:138-141` |
| `ua_publishing_interval_ms` | 100 | publishing interval for the single shared subscription |

ProductUri is fixed to `urn:opensmi:client` (`:133`). The constructor creates a `SubscriptionManager` (`:149-151`).

**Public attributes and properties**
- `subscription_manager: SubscriptionManager` (`:149`).
- `custom_type_definitions: ClassVar[dict[str,type]]`. This is **global across instances** because asyncua stores loaded types globally (`:76`, `:358-364`; docstring warning `:69-72`).
- `init_exceptions: list[Exception]` holds machine-level init failures (`:165`, `:442`).
- `browse_rules` (`:168-171`), `ua_client` (the raw asyncua `Client`, `:385-388`), `path` (always `/`, `:400-403`), `status` (`:444-447`), `connected` (true iff the asyncua `UaClientState.CONNECTED`, `:449-452`), `machines: dict[str, RemoteMachine]` keyed by BrowseName.Name (`:454-457`), `username` (`:459-462`).
- Inherited from `BaseServer` (`core/base_server.py:21-75`): `get_ua_object(node_id)` (raises KeyError), `get_ua_object_optional(node_id)`, `register_ua_object`, `unregister_ua_object`, `ua_get_namespace_index(uri|NamespaceProvider) -> Int16` (raises KeyError), `ua_get_node_id(NodeIdDefinition)`.

**Signals**
- `status_changed: Signal[RemoteServerStatus]` fires only on an actual change (`:160`, `:173-179`).
- `time_updated: Signal[datetime]` fires on each data change of `Server_ServerStatus_CurrentTime` (i=2258). Usable as a heartbeat (`:162`, `:390-398`).

**Methods**
- `async connect(*, max_retries: int|None=3, min_delay_seconds=0.5, max_delay_seconds=30.0) -> Self` (`:181-224`). Runs `_connect_and_retry` as a task. Raises `RuntimeError` if a connect is already in progress or the status is not DISCONNECTED (`:196-201`). If the caller cancels (e.g. `wait_for`), it tears down to DISCONNECTED and re-raises. If `disconnect()` aborted it, it raises `ConnectionAbortedError` (`:210-221`).
  - The retry loop (`:226-270`) uses exponential backoff with the delay doubling up to the max. `BadIdentityTokenRejected` is retried (user logged in elsewhere). `BadUserAccessDenied`/`BadIdentityTokenInvalid` raise `ConnectionRefusedError` immediately. `OSError`/`UaError` are retried. After `max_retries` it raises `ConnectionError` (`None` = retry forever).
  - `_connect` sequence (`:351-383`):
    1. Set security.
    2. `ua_client.connect()`.
    3. Load custom data types; a failure only logs a warning.
    4. Build `namespace URI -> index` from the NamespaceArray.
    5. Clear `machines` and `_ua_objects`.
    6. `subscription_manager.clear_subscriptions()` then `init()`.
    7. If `browse_rules` is truthy: set status BROWSING, then `_browse_machines()`.
    8. Subscribe to CurrentTime.
    9. Set status CONNECTED.
- `async disconnect()` (`:289-309`): cancels the status task, sets DISCONNECTING, cancels a pending connect, shuts down subscriptions, disconnects the UA client, and always ends in DISCONNECTED. Remote objects are **not** `shutdown()`.
- `async __aenter__` calls `connect()` with the defaults and returns self. `__aexit__` calls `disconnect()` (`:484-491`).
- `async ua_on_data_change(...)` forwards to `time_updated` (`:396-398`).

**Status tracking**: a background task follows asyncua's `UaClientState` and maps it onto `RemoteServerStatus` (`:464-482`). `CONNECTED` coming from asyncua is suppressed until browsing finishes (`_suppress_connected`, `:144`, `:353`, `:382`, `:466-468`). An asyncua auto-reconnect produces `RECONNECTING` and then `CONNECTED`, but it does **not** re-browse. Re-browsing happens only in `_connect`. Subscriptions are recovered by the `SubscriptionManager` heartbeat (§7). Whether asyncua itself transfers subscriptions on reconnect is not handled here and is unclear.

### 2.3 `BaseRemoteComponent` (`client/remote_component.py:61-418`)
Bases: `RemoteUaObject["BaseRemoteComponent"]`, `UaDataChangeSubscriber`, `UaEventSubscriber`. Represents a server-side `BaseMachineryItem`.

- ctor `(*, name, ua_node: Node, parent, server, browse_rules, ua_type=None, **kw)` (`:68-108`).
- Public attributes (`:90-105`):
  - `skill_set: dict[str, RemoteSkill]`, built from `SkillExecution` children.
  - `feasibility_check_set: dict[str, RemoteSkill]`, built from `FeasibilityCheck` children.
  - `precondition_check_set: dict[str, RemoteSkill]`, built from `PreconditionCheck` children.
  - `components: dict[str, RemoteComponent]`, `method_set: dict[str, RemoteMethod]`, `resources: dict[str, RemoteResource]`.
  - Variable containers: `identification`, `monitoring`, `attributes`, `parameter_set`. They are always present; they stay empty and uninitialized if the server lacks the node or the feature is not browsed.
- Signals (`:107-108`):
  - `status_changed: Signal[BaseRemoteComponent, str]` carries the MachineryItemState CurrentState text (`:394-403`).
  - `notification_received: Signal[BaseRemoteComponent, NotificationEvent]` (`:372-392`).
- Properties: `current_state: str`, cached, default `"Unknown"` (`:110-113`). `name` (getter, plus a setter that accepts LocalizedText) (`:124-134`). `type` returns `ua_type`, "for UI" (`:136-139`).
- `async read_current_state() -> str` reads the `MachineryItemState/CurrentState` node directly, or returns `"Unknown"` if that node is absent (`:115-122`). Note: it returns the raw value (LocalizedText or str), unlike the cached `.Text` handling in `ua_on_data_change`.
- Deprecated: `read_parameters()`, `write_parameters(mapping)`, `read_monitoring()` (`:405-418`).
- `_init` browse algorithm: see §3. It ends by batch-subscribing every variable in `monitoring`, `attributes`, `parameter_set` and `identification` (`:366-370`).

### 2.4 `RemoteComponent(BaseRemoteComponent)` (`client/remote_component.py:421-449`)
- `lock -> RemoteLock | None`. The lock is optional for components (`:424-427`).
- `dto() -> RemoteComponentDTO` (`:429-449`).

### 2.5 `RemoteMachine(BaseRemoteComponent)` (`client/remote_machine.py:29-112`)
- ctor `(*, name, ua_node, server, browse_rules=None)`. `parent` is always `None` (`:32-49`).
- `lock -> RemoteLock` **asserts** that it is not None, because the lock is mandatory for machines (`:51-55`). If the `LOCK` feature was not browsed, accessing `lock` (and therefore calling `dto()`, `:66`) raises `AssertionError`.
- `users: list[RemoteUser]` (`:81-84`). Users are browsed from `MachineryBuildingBlocks/Users` only when `BrowseFeature.USERS` is set (`:97-112`).
- `dto() -> RemoteMachineDTO` (`:57-79`).

### 2.6 `RemoteResource(BaseRemoteComponent)` (`client/remote_resource.py:13-26`)
Browses the same way as a component (it inherits `_init`). `dto() -> RemoteResourceDTO` contains only attributes, identification and monitoring.

### 2.7 `RemoteCallable` (abstract; `client/remote_callable.py:33-152`)
Base of `RemoteMethod` and `RemoteSkill`. Parent: `BaseRemoteComponent`.
- ctor `(*, name, ua_node, server, parent, browse_rules)` (`:36-54`).
- Attributes: `final_result_data: FinalResultData`, `monitoring: Monitoring`, `parameter_set: ParameterSet` (`:49-51`).
- `_init` browses the children of `ua_node` (`:56-94`):
  - `MinAccessLevel` is read once.
  - `Monitoring`, `ParameterSet` and `FinalResultData` are each gated by their browse feature.
  - `Requirements` is processed by `_browse_dependencies`, which reads the `Requires` (i=…`ObjectIds.Requires`) references; forward references go to `depends_on` and inverse ones to `dependency_of` (`:125-132`).
  - Any other child is passed to the abstract `_init_handle_node`.
  - Finally all container variables are batch-subscribed (`:91-94`).
- Properties: `min_access_level: int|None` (`:107-113`), `ua_depends_on: Sequence[NodeId]` (`:115-118`), `ua_dependency_of` (`:120-123`).
- Deprecated: `read_results`, `read_parameters`, `write_parameters`, `read_monitoring` (`:134-152`).

### 2.8 `RemoteMethod(RemoteCallable)` (`client/remote_method.py:20-56`)
- `_init_handle_node` records the NodeId of the `Call` child (`:40-48`).
- `async call() -> Mapping[str, Any]` calls the OPC UA method `Call` on the method object with **no input arguments**, then returns `final_result_data.read_all()`. Parameters are passed by writing `parameter_set` first. Errors propagate as raw asyncua exceptions ("User is responsible", `:50-56`). If the `Call` node is missing, `_ua_call` is unset and `call()` raises `AttributeError` (this is implicit, inferred from `:23`).
- `dto() -> RemoteCallableDTO` (`:25-38`).

### 2.9 `RemoteSkill(RemoteCallable, UaDataChangeSubscriber)` (`client/remote_skill.py:58-262`)
- `ua_node` is the **`SkillExecution`** (or `FeasibilityCheck`/`PreconditionCheck`) sub-node, not the skill object (`client/remote_component.py:144-160`). `name` is the BrowseName of the skill object (the parent folder).
- ctor `(*, name, ua_node, server, parent, browse_rules)`. Initial cached state is `SkillState.HALTED` (`:63-78`).
- Signal: `state_changed: Signal[RemoteSkill, SkillState]` fires on every change of the cached state (`:78`, `:217-233`).
- Properties: `current_state: SkillState` (cached, `:240-243`), `suspendable: bool` (true iff a `Suspend` method node exists, `:145-148`), `is_finite: bool` (true iff `ua_type.lower().startswith("finite")`, `:259-262`).
- `async ua_read_type()` is overridden to read the type definition of the **parent node** (the skill object, e.g. `FiniteSkillType`/`ContinuousSkillType`) (`:250-257`).
- Methods:
  - `async start(*, wait=False)`: waits for RUNNING **or** COMPLETED (`:150-158`).
  - `async reset(*, wait=False)`: waits for READY (`:160-168`).
  - `async halt(*, wait=False)`: waits for HALTED (`:170-175`).
  - `async suspend(*, wait=False)`: raises `SkillNotSuspendableError` if there is no Suspend node (`:177-189`).
  - `async wait_for_state(target_state)`: "Advanced". Returns immediately if already in the target state. Raises `SkillHaltedError` if the skill halts while waiting for a non-HALTED state. Can hang on transient states; bound it with `asyncio.timeout()` (`:191-202`).
  - `async read_current_state() -> SkillState`: direct read (`:245-248`). It raises `KeyError` for an unknown state string, because `_parse_state` is not guarded there.
- `dto() -> RemoteSkillDTO` (`:80-96`).
- Mandatory child nodes of `StateMachine` (asserted): `CurrentState`, `Halt`, `Reset`, `Start`. `Suspend` is optional (`:98-133`). Details in §5.

### 2.10 `RemoteVariable(RemoteUaObject["RemoteVariableContainer"], UaDataChangeSubscriber)` (`client/remote_variable.py:78-381`)
- ctor `(*, name, ua_node, server, parent)` (`:83-106`).
- `_init` (`:108-175`):
  1. Reads DataType, Value, ValueRank and UserAccessLevel in one call.
  2. `is_writable` = `CurrentWrite in UserAccessLevel` (per-user access level, `:127-131`).
  3. `ua_data_type` = the ns0 type name from `ObjectIdNames` (`:133-139`).
  4. If the value is a NodeId, `valid_values` = `{"": "<None>"}` plus every `Utilizes` (i=…`ObjectIds.Utilizes`) reference target as `nodeid string -> DisplayName` (`:143-149`).
  5. Otherwise, for a non-ns0 DataType, `ua_data_type` = the type BrowseName and the Python class is looked up in `RemoteServer.custom_type_definitions` (`:150-163`).
  6. Reads the `EngineeringUnits` and `EURange` properties (`:168-170`, `core/ua_node_util.py:101-128`).
  7. Seeds the cache from the value read. The subscription is set up later, batched by the container (`:172-175`).
- Public attributes: `unit: ua.EUInformation|None`, `range: ua.Range|None`, `children: dict[str, RemoteVariable]` (declared but never filled in the client code).
- Signal: `value_changed: Signal[RemoteVariable, ua.Variant]` fires when the cached Variant changes (`:106`, `:324-340`).
- Properties: `is_writable` (`:177-183`), `value` (processed, cached; `:207-210`), `raw_value: Variant` (`:212-215`), `is_value_node_id` (`:217-220`), `valid_values: dict[int,str]|dict[str,str]` (an IntEnum custom type gives `{value: name}`, `:259-268`), `timestamp: datetime|None` (ServerTimestamp, `:270-273`), `ua_data_type: str`, `ua_value_rank: int` (`:275-283`).
  - Note: `_ua_value_rank` is never assigned from the read. `_init` stores it as `_ua_value_rank_value` (a DataValue, `:116`), so `ua_value_rank` always returns 0. This is a client bug.
- Value processing in `_process_value` (`:185-205`):
  - None or a null NodeId gives `None`.
  - An IntEnum custom type gives the enum member, or `None` if invalid.
  - When `valid_values` is non-empty and the value is str/int, the result is the mapped display name, or `"INVALID ENUM VALUE: x"`.
  - Otherwise the raw `.Value` is returned.
- Methods:
  - `read_value(*, update_cache=False)` (`:222-227`).
  - `read_raw_value() -> Variant` (`:229-232`).
  - `read_resolved_value()`: for a NodeId value it returns the target's DisplayName, falling back to BrowseName and then to the last `.`-segment of a string identifier (`:234-257`).
  - `read_history(start_time=None, end_time=None, num_values=0, return_bounds=True) -> list[TimestampedValue]` (`:302-322`).
  - `update_from_data_value(DataValue|DataChangeNotif|None)` (`:324-340`).
  - `write_value(value)` (`:342-367`).
  - `__str__` gives the value, unit and range (`:373-381`).
- `write_value` semantics:
  - Raises `RuntimeError` if not writable.
  - The VariantType is taken from a passed `ua.Variant`, else from the cached `raw_value.VariantType`.
  - Uses `core/ua_node_util.write_value`, which converts a str to NodeId for NodeId variants (`""`/None become a null NodeId) and a str to LocalizedText(`en-US`) (`core/ua_node_util.py:46-82`).
  - `BadUserAccessDenied` and `BadOutOfRange` are re-raised.
  - On any other `UaStatusCodeError`, if the value is a str found among the `valid_values` *display names*, the corresponding key is written. Otherwise it raises `RuntimeError("… is not a valid value …")`.
- `TimestampedValue` dataclass: `value`, `timestamp` (`:70-75`).

### 2.11 `RemoteVariableContainer[_ParentType]` and subclasses (`client/remote_variable_container.py:26-146`)
- ctor `(*, name, ua_node: Node|None, parent, server, **kw)`. Subclasses are constructed with `ua_node=None`; the parent sets `container.ua_node = child` before calling `init()`.
- `_init`: for each child, if the server registry already holds a `RemoteVariable` for that NodeId, that object is **reused**. Otherwise a new `RemoteVariable` is created, `init()`ed and `ua_read_type()`d (`:52-64`).
- `async read_all() -> Mapping[str, Any]` reads **sequentially**, one request per variable (`:66-68`). `async write_all(Mapping[str, Any])` writes sequentially and raises `KeyError` for unknown names (`:70-73`).
- `dto() -> list[RemoteVariableDTO]` returns `[]` if the container is not initialized (`:75-79`).
- Python container protocol: `__getitem__` (KeyError with path), `__contains__`, `__len__`, `__iter__` over the `RemoteVariable`s (`:85-106`).
- Subclasses with fixed names: `Attributes`, `Identification`, `Monitoring`, `ParameterSet`, `FinalResultData` (`:109-146`).

### 2.12 `RemoteLock(RemoteUaObject["BaseRemoteComponent"], UaDataChangeSubscriber)` (`client/remote_lock.py:60-509`)
- ctor `(*, parent, server, ua_node=None, max_inactive_lock_time: int=60_000, renew_threshold: float=0.5, renew_retry_seconds: float=1.0)`. `name` is fixed to `"Lock"` (`:97-133`).
- Properties (all cached, eventually consistent; see the docstring `:61-90`): `locked`, `locking_user`, `locking_client`, `locked_since`, `locked_by_us` (= `locked and locking_user == server.username`), `remaining_lock_time_ms` (None when unlocked), and `ua_nodes: RemoteLockUaNodes` (`:152-192`, `:381-384`).
- Validated settable properties: `max_inactive_lock_time_ms` (>0), `renew_threshold` (0<x<1) and `renew_retry_seconds` (>0). Invalid values raise `ValueError` (`:194-235`).
- Methods: `init_lock()`, `break_lock()`, `exit_lock()`, `renew_lock()`, and `read_state(*, update_cache=True) -> RemoteLockState` (`:239-278`, `:459-509`). See §6.
- Signals:
  - `locked_changed: Signal[RemoteLock, bool]`
  - `locking_user_changed: Signal[RemoteLock, str]`
  - `renew_due: Signal[RemoteLock, float]` (payload: remaining ms) (`:128-133`)
- `dto() -> RemoteLockDTO|None` returns None if not initialized (`:135-150`).
- Helper dataclasses: `RemoteLockUaNodes` (Locked, LockingUser, LockingClient, RemainingLockTime, InitLock, BreakLock, ExitLock, RenewLock; `:32-45`) and `RemoteLockState` (locked, locked_since, locking_user, locking_client, remaining_lock_time_ms, locked_by_us; `:48-57`).

### 2.13 `AutoLock` (`client/auto_lock.py:15-125`)
- ctor `(lock: RemoteLock, *, break_lock=False, interval=1.0)`.
- Methods: `async lock(*, blocking=True)` (`:37-50`), `async unlock()` (`:52-74`), and the signal handlers `on_locking_user_changed` and `on_renew_due` (`:102-120`).
- Property `is_active` (`:122-125`).
- It is not an async context manager. See §6.

### 2.14 `RemoteUser(RemoteUaObject["RemoteMachine"], UaDataChangeSubscriber)` (`client/remote_user.py:46-102`)
- ctor `(*, name, ua_node, server, parent: RemoteMachine, **kw)`.
- `data: RemoteUserData` (`:61`), a dataclass with fields `name`, `allow_multiple: bool`, `user_level: str` ("priority … for breaking locks"), `max_access_level: int` and `is_present: bool` (`:29-43`).
- `_init` reads the children `AllowMultiple`, `UserLevel`, `MaxAccessLevel` and `IsPresent`, and subscribes to `IsPresent` only (`:64-78`).
- Signal: `user_presence_changed: Signal[RemoteUser, bool]` (`:62`, `:97-102`).
- `dto() -> RemoteUserDTO` maps `current_access_level=int(data.user_level)` (`:80-91`). This is a naming mismatch: the server writes `UserLevel = str(user.priority)` (`server/user.py:103-104`), so the DTO's "current_access_level" actually carries the lock priority. `int()` raises if UserLevel is empty or non-numeric.
- `__str__` (`:93-95`).

### 2.15 `NotificationEvent` (frozen dataclass, `client/remote_component.py:43-52`)
Fields: `message: str`, `severity: int` (0 to 1000), `error_code: str` (may be empty).

### 2.16 DTOs (`client/dto.py`; all `@dataclass(slots, frozen, kw_only)`)

| DTO | Fields | Line |
|---|---|---|
| `RemoteUaObjectDTO` | name, path: PurePosixPath, ua_node_id: str (e.g. `ns=1;s=DummyMachine`), ua_type | `:13-21` |
| `RemoteUserDTO` | + allow_multiple, current_access_level, max_access_level, is_present | `:24-31` |
| `RangeDTO` | low, high | `:34-41` |
| `RemoteVariableDTO` | + value, timestamp, is_writable, valid_values, unit (symbol = EUInformation.DisplayName.Text), range: RangeDTO\|None, ua_data_type, ua_value_rank | `:44-62` |
| `RemoteCallableDTO` | + min_access_level, parameter_set, monitoring, final_result_data (lists of RemoteVariableDTO), ua_depends_on_node_ids, ua_dependency_of_node_ids | `:65-74` |
| `RemoteSkillDTO` | + current_state (SkillState name, e.g. `READY`), is_suspendable, is_finite | `:77-85` |
| `RemoteResourceDTO` | + attributes, identification, monitoring | `:88-94` |
| `RemoteLockDTO` | + is_locked, locking_user, locking_client, locked_since, remaining_lock_time_ms, max_inactive_lock_time_ms | `:97-114` |
| `RemoteComponentDTO` | + current_state, lock: RemoteLockDTO\|None, attributes, identification, monitoring, parameter_set, skill_set: list[RemoteSkillDTO], method_set: list[RemoteCallableDTO], components (recursive), resources | `:117-132` |
| `RemoteMachineDTO` | + users | `:135-139` |

DTOs are snapshots built from **cached** values; `dto()` performs no I/O. `feasibility_check_set` and `precondition_check_set` are **not** included in any DTO. The JSON conversion (datetime to ISO, path to str) is left to the user (`osc/examples/dto.py:16-32`).

### 2.17 `BrowseFeature` / `BrowseRule` (`client/browse.py`)
- `BrowseFeature(Flag)`: `MACHINES, COMPONENTS, LOCK, RESOURCES, USERS, SKILL_SET, METHOD_SET, ATTRIBUTES, IDENTIFICATION, MONITORING, PARAMETER_SET, FINAL_RESULT_DATA` (`:16-33`).
- `BrowseRule(match: Callable[[PurePosixPath], bool], features: BrowseFeature)` is a frozen dataclass (`:36-44`).
- `browse_when(match, *features) -> BrowseRule` ORs the given features together (`:47-56`).
- `evaluate_browse_features(rules, path) -> BrowseFeature` ORs the features of every matching rule; `BrowseFeature(0)` if none match (`:59-69`).
- Matchers: `match_all`, `match_name(name)` (compares the last path segment), `match_path(path)` (exact), `match_path_prefix(prefix)` (the path itself or a descendant), `match_any(*m)`, `match_all_of(*m)`, `match_not(m)` (`:77-111`).
- `DEFAULT_BROWSE_RULE = BrowseRule(match_all, ~BrowseFeature(0))` enables every feature (`:114-117`).

### 2.18 Core `Signal[P]` (`core/signal.py:16-36`)
`connect(cb)`, `disconnect(cb)` (both use a set, so callbacks are de-duplicated and order is undefined), and `async send(*args)`. `send` snapshots the callbacks and awaits them **concurrently** with `asyncio.gather` (without `return_exceptions`), so an exception in one callback propagates to the sender. For data-change signals that sender is the SubscriptionManager, which logs and swallows handler exceptions (`core/subscription_manager.py:418-421`).

---

## 3. Browsing

**Entry point** (`client/remote_server.py:409-442`):
1. Get the node `ns=<idx of "http://opcfoundation.org/UA/Machinery/">;i=1001` ("Machines", defined at `nodesets/Opc.Ua.Machinery.NodeSet2.xml:1622`) and list its children.
2. If that fails or returns no children, fall back to the first child of `Objects` whose BrowseName.Name is `"Machines"`, ignoring the namespace. This raises `BadNoMatch` if there is none (`:423-427`; `core/ua_node_util.py:181-204`).
3. For each child, evaluate the rules on path `/<BrowseName.Name>`. If `MACHINES` is enabled, create a `RemoteMachine` and `init()` all of them concurrently. Successful machines go into `machines` and failures into `server.init_exceptions` (`:429-442`).

**Component browse** (`BaseRemoteComponent._init`, `client/remote_component.py:296-370`): evaluate the features once for `self.path`, then dispatch on the **BrowseName.Name of each child, ignoring the namespace**:

| Child BrowseName | Gate | Action |
|---|---|---|
| `Lock` | LOCK | `RemoteLock` init + type (`:178-184`) |
| `SkillSet` | SKILL_SET | for each skill child, then each grandchild `SkillExecution` / `FeasibilityCheck` / `PreconditionCheck`, create a `RemoteSkill` (`:141-176`) |
| `Notification` | always | subscribe to events; on `BadServiceUnsupported`, retry on the `Messages` child (legacy "v4 server") (`:285-294`) |
| `Identification`, `Attributes`, `Monitoring`, `ParameterSet` | matching feature | container init (`:319-342`) |
| `Components` | COMPONENTS | each child becomes a `RemoteComponent` (its type is read first) (`:186-217`) |
| `Resources` | RESOURCES | each child becomes a `RemoteResource` (`:219-238`) |
| `MachineryBuildingBlocks` | always | `MachineryItemState` causes a subscription to its `CurrentState`; other children go to `_init_machinery_building_blocks_handle_node`, which `RemoteMachine` overrides to handle `Users` (USERS gate) (`:252-264`; `client/remote_machine.py:97-112`) |
| `MethodSet` | METHOD_SET | each child becomes a `RemoteMethod` (`:266-283`) |
| anything else | – | warning log (`:360-361`) |

All sub-browses for one object run concurrently through `common.gather`. `ConnectionError`, `TimeoutError` and non-`Exception` BaseExceptions are raised; any other `Exception` is **collected** into `init_exceptions` and the failed child is simply missing (`client/common.py:16-33`). Errors from component, resource, method and variable children get a note naming the node (`add_note`).

**Path model used by the rules** (derived from `BaseUaObject.path`, `core/base_ua_object.py:91-97`):
- Machine: `/M`. Component: `/M/C` (the `Components` folder is **not** a path segment). Nested component: `/M/C/C2`. Resource: `/M/R`. Skill or method: `/M/SkillName` (parent is the component; `SkillSet`/`MethodSet` are not segments). Lock: `/M/Lock`. Container: `/M/ParameterSet`. Variable: `/M/C/Monitoring/Var`.
- Rules are evaluated at the server (MACHINES on `/M`), at each component, machine or resource (`self.path`), and at each callable (for MONITORING, PARAMETER_SET and FINAL_RESULT_DATA; `client/remote_callable.py:60`). Locks, containers, variables and users do not evaluate rules themselves.
- Consequence (example `osc/examples/browse_rules.py:17-29`): `match_name("DummyMachine")` grants COMPONENTS and SKILL_SET only to `/DummyMachine`. Its components receive only what `match_all` grants.

**"Browse nothing" and direct node-id mode**: with `browse_rules=()`, the BROWSING step is skipped and `machines` stays empty (`client/remote_server.py:376-378`). The user can then build any remote object from a known NodeId: `await RemoteComponent(name=…, ua_node=server.ua_client.get_node("ns=1;s=DummyMachine.Components.DummyComponent"), parent=None, server=server, browse_rules=(DEFAULT_BROWSE_RULE,)).init()` (`osc/examples/direct_component.py:18-31`). With `parent=None` the path becomes `/DummyComponent`. The README describes this as "point the client directly at them and skip discovery entirely, or mix both" (`osc/README.md:23-25`). The server's string NodeId convention is `<parentId>.<Name>` with the `ns` of the application namespace, usually 1 (`core/ua_node_util.py:149-178`; `server/ua_object.py:93`; `server/_server.py:289`).

**Caching and identity**:
- `server._ua_objects` is a `WeakValueDictionary[NodeId, RemoteUaObject]`, filled when `ua_node` is set (`core/base_server.py:28`, `core/base_ua_object.py:68-76`). It is cleared on every `_connect` (`client/remote_server.py:371-372`).
- `RemoteVariableContainer` reuses an already-registered `RemoteVariable` with the same NodeId (`client/remote_variable_container.py:56-60`).
- All state (variable values, skill and component states, lock fields, user presence) is cached and updated by subscription. There is no re-browse after a reconnect (§2.2).

**Type resolution**: `ua_type` is the BrowseName.Name of the HasTypeDefinition target, or `"UNKNOWN"` (`core/ua_node_util.py:131-146`).

---

## 4. Information model expected by the client

### 4.1 Namespace URIs

| URI | Client use | Server definition |
|---|---|---|
| `http://opcfoundation.org/UA/Machinery/` | `Machines` folder `i=1001` (`client/remote_server.py:414-417`) | `nodesets/machinery.py:22-24` (v1.03.0); XML `Opc.Ua.Machinery.NodeSet2.xml:1622` |
| `http://opcfoundation.org/UA/DI/` | `MaxInactiveLockTime` `i=6387` (`client/remote_lock.py:282-286`) | `nodesets/di.py:22-23` (v1.04.0); server writes it at `server/lock.py:204-207` |
| `http://smartfactory.de/UA/MachineSet` | not referenced by URI in the client (matching is by name only) | `nodesets/smart_factory_machine_set.py:22-29` (v1.00) |
| `http://smartfactory.de/UA/SkillSet` | not referenced by URI in the client | `nodesets/smart_factory_skill_set.py:22-24` (v1.00) |
| `http://opcfoundation.org/UA/RSL/` | not used by the client | `nodesets/rsl.py:22-23` |
| server application namespace (`config.namespace_uri`, default `urn:smartfactory.de:machine:XYZ`) | not used; instance NodeIds live there (ns 1) | `server/config.py:49`; `server/_server.py:289` |

The registry of the server's nodesets is at `server/nodesets/__init__.py:21-25`. Apart from the two NodeIds above, **the client matches every node by `BrowseName.Name` and ignores namespace indices.** It also uses ns0 well-known ids: `Server_ServerStatus_CurrentTime`, `Server_ServerCapabilities_OperationLimits_MaxMonitoredItemsPerCall`, reference types `Requires`, `Utilizes` and `HasTypeDefinition`.

### 4.2 Address-space structure (as the client browses it, verified against the server)

```
Objects/
  Machines  (Machinery ns, i=1001)                       server/_server.py:179, :297-307
    <MachineName>  (MachineType, MachineSet i=1009)      server/base_machine.py:59-61
      Lock  (DI LockingServicesType, HasAddIn)           server/lock.py:144-148; server/mixins/lockable_mixin.py:21-24
        Locked, LockingUser, LockingClient, RemainingLockTime   (vars)  server/lock.py:163-180
        InitLock, ExitLock, BreakLock, RenewLock                (methods)
      Identification  (Machinery MachineIdentificationType)     server/ua_variable_containers.py:381-394
      Attributes      (DI FunctionalGroupType)                  :319-332
      Monitoring      (MachineSet MonitoringType)               :304-317
      ParameterSet    (DI FunctionalGroupType)                  :289-302
      Notification    (MachineSet NotificationType)             server/notification.py:34-39
      MachineryBuildingBlocks                                    server/base_machinery_item.py:115
        MachineryItemState / CurrentState  (LocalizedText)       :117-121
        MachineryOperationMode / CurrentState  (not read by client)
        Users  (MachineSet UsersType)                            server/base_machine.py:70-76; server/ua_object_containers.py:176-187
          <UserName>  (UserType i=1023): Name, UserRole, AllowMultiple, UserLevel(str), MaxAccessLevel(Int32), IsPresent(bool), Language
                                                                 server/user.py:86, :96-113
      SkillSet  (SkillSet SkillSetType, HasAddIn)                server/ua_object_containers.py:125-138
        <SkillName>  (FiniteSkillType i=1015 | ContinuousSkillType i=1016)   server/mixins/skill_logic_mixins.py:377, :412
          Name
          SkillExecution | FeasibilityCheck | PreconditionCheck  (SkillElementType i=1013)
                                                                 nodesets/Opc.Ua.SmartFactory.MachineSet.NodeSet2.xml:1841-1915
            MinAccessLevel (Int32)                               server/base_callable.py:73-84
            ParameterSet / Monitoring / FinalResultData          server/ua_variable_containers.py:289-347
            Requirements  (Requires refs)                        server/mixins/requirements_mixin.py:50-67
            StateMachine  (SkillStateMachineType i=1014)          server/base_skill.py:75
              CurrentState (LocalizedText), Start, Halt, Reset, [Suspend]   server/base_skill.py:76-97
      MethodSet  (SkillSet MethodSetType, HasAddIn)              server/ua_object_containers.py:141-154
        <MethodName>  (BaseMethodType, SkillSet i=1004)          server/base_method.py:25-40
          Call (method), MinAccessLevel, ParameterSet, Monitoring, FinalResultData, Requirements
      Components  (MachineComponentsType)                        server/ua_object_containers.py:157-171
        <ComponentName>  (ComponentType i=1008 or subtype)       server/base_machinery_item.py:456
          (same children as a machine minus Users; Lock optional; Notification optional, otherwise forwarded to the parent)
      Resources  (MachineSet ResourcesType, HasAddIn)            server/ua_object_containers.py:95-108
        <ResourceName>  (ResourceType i=1011)                    server/resource.py:83
```

Notes:
- **Callable containers live under the sub-node.** For callables the server's `ua_node` returns `_ua_sub_node` (`server/base_callable.py:106-112`) and the container lifecycles run after the sub-node is resolved (`:58-64`). ParameterSet, Monitoring, FinalResultData and MinAccessLevel are therefore children of `SkillExecution` (skills) or of the method object itself (methods, `server/base_method.py:29-31`). The client mirrors this by using `SkillExecution` as the skill's `ua_node`.
- **`Requirements` placement is unclear.** The SkillSet nodeset declares `Requirements` on `BaseSkillType` (`nodesets/Opc.Ua.SmartFactory.SkillSet.NodeSet2.xml:276`, parent `ns=2;i=1001`). The client looks for it only among the children of `SkillExecution` (`client/remote_callable.py:85-86`). The server's `RequirementsMixin` resolves it via `self.ua_node` (`server/mixins/requirements_mixin.py:57-67`), which for callables is the sub-node once it is set. Whether a declared `Requirements` sits at skill level or SkillExecution level in practice depends on when `_init_requirements` runs; this was not verified.
- State, method and variable child names come from the nodesets: state machine `Start`/`Halt`/`Reset`/`Suspend` at `Opc.Ua.SmartFactory.MachineSet.NodeSet2.xml:1197,1272,1332,1365`; `Call` at `Opc.Ua.SmartFactory.SkillSet.NodeSet2.xml:776`; `MinAccessLevel` at `:836,:846`.

---

## 5. Skill state machine

**States** (`core/ua_enums.py:21-60`; `SkillState` value = `(id, text)`):

| Enum | id | Text | Kind |
|---|---|---|---|
| HALTED | 11 | Halted | stable (initial) |
| READY | 12 | Ready | stable |
| RUNNING | 13 | Running | stable (continuous) |
| SUSPENDED | 14 | Suspended | stable |
| COMPLETED | 15 | Completed | stable (finite only) |
| STARTING | 16 | Starting | transient |
| HALTING | 17 | Halting | transient |
| COMPLETING | 18 | Completing | transient (finite only) |
| RESETTING | 19 | Resetting | transient |
| SUSPENDING | 20 | Suspending | transient |

The state node objects in the nodeset are children of `SkillStateMachineType` ns=5;i=1014, for example `Halted` i=5114, `Ready` i=5119, `Running` i=5127, `Suspended` i=5140, `Completed` i=5108, `Starting` i=5133, `Halting` i=5117, `Resetting` i=5124, `Completing` i=5147 and `Suspending` i=5151 (`nodesets/Opc.Ua.SmartFactory.MachineSet.NodeSet2.xml:1171-1714`). The ids 11-20 come from the enum; the client never uses them.

**Client parsing**: `CurrentState` is a LocalizedText (or a plain str on some servers). The parser upper-cases the text and looks it up with `SkillState[...]`. An **empty text maps to HALTED** ("assume the worst"). An unknown text raises `KeyError`, which is logged and ignored in subscription handling (`client/remote_skill.py:46-55`, `:217-222`).

**Transitions (server, `server/mixins/skill_logic_mixins.py:83-137`, finite additions `:415-423`)**

| Trigger (OPC UA method) | From | To | Server conditions |
|---|---|---|---|
| `Reset` | HALTED, STARTING, SUSPENDED, COMPLETED | RESETTING, then READY (`resetting_done`) | access allowed, reset allowed |
| `Halt` | RESETTING, READY, STARTING, RUNNING, COMPLETED, SUSPENDED, SUSPENDING | HALTING, then HALTED | access allowed |
| `Start` | READY, SUSPENDED | STARTING, then RUNNING | access allowed, startup completed, dependencies ready, start allowed |
| `Suspend` | RUNNING | SUSPENDING, then SUSPENDED | access allowed, suspendable, suspend allowed |
| (internal, finite) | RUNNING → COMPLETING → COMPLETED | – | `done_internal` |

- A failure in any handler triggers `halt` (`:175-200`, `fail_trigger="halt"`).
- "Access allowed" requires either an Admin role, or a `current_access_level >= MinAccessLevel` **and** being the owner of the lock. Otherwise the server raises `BadUserAccessDenied` / `BadRequiresLock` (code `E123`) (`server/base_callable.py:118-134`).
- Results of the server method call (`server/ua_fsm.py:94-117`): Good. A wrong state gives `BadInvalidState`. A condition failure gives its UaStatusCode. Any other error gives `BadInternalError`.
- The client calls `call_method(StateMachine, <Start|Reset|Halt|Suspend NodeId>)` with no arguments (`client/remote_skill.py:156,166,173,184`). A bad status code surfaces as an asyncua `UaStatusCodeError` subclass.
- `Suspend` exists only on suspendable skills. The server deletes it otherwise (`server/base_skill.py:90-99`), and the client derives `suspendable` from its presence.
- The server enforces that a machine has a continuous `StartupSkill` (`server/base_machine.py:78-93`). In the examples the client starts it before other skills (`osc/examples/example.py:36-41`).

**How `wait=True` works** (`client/remote_skill.py:150-215`):
1. The current `_halt_count` (incremented on every transition into HALTED) is captured **before** the method call.
2. The method is called.
3. Under an `asyncio.Condition` the client waits until the cached `current_state` (updated only by the CurrentState subscription, `:217-238`) is in the target set, or until `_halt_count` has grown when HALTED is not a target. In the halt case it raises `SkillHaltedError`.

Targets: start → {RUNNING, COMPLETED}; reset → {READY}; halt → {HALTED}; suspend → {SUSPENDED}.

There is **no timeout**; callers must wrap the call in `asyncio.timeout`. Because the wait checks only the latest cached state, a fast finite skill that passes RUNNING still satisfies the wait through COMPLETED. **`start(wait=True)` does not wait for completion of a finite skill**, only until it is RUNNING or COMPLETED. The README and quick_start comment "start the skill and wait for completion" (`osc/README.md:63-64`, `osc/examples/quick_start.py:30-31`) is therefore inaccurate for skills that stay in RUNNING for a while. If the cached state already equals a target state before the call (e.g. `reset(wait=True)` while READY is cached), the wait returns immediately.

---

## 6. Lock semantics

**Server rules** (`server/lock.py`):
- `InitLock` succeeds only if the lock is unlocked and the user has access. Otherwise it returns `BadLocked`, or `BadUserAccessDenied` (`:77-88`, `:281-294`). The server method accepts an optional `context` argument; the client sends none.
- `BreakLock` requires access and either an Admin role or `user.priority > locking_user.priority`. Otherwise `BadUserAccessDenied`. It returns 0, or -1 if the lock was not locked (`:90-104`, `:313-327`).
- `ExitLock` requires being the lock owner, else `BadUserAccessDenied`. If not locked it returns the integer `-1` (E_NotLocked) **without a bad status** (`:106-117`, `:297-310`).
- `RenewLock` requires ownership. It resets the inactivity timer and returns -1 when not locked (`:119-125`, `:331-344`).
- An inactivity loop updates `RemainingLockTime` every second and releases the lock when it reaches 0 (`:127-140`). The default `max_inactive_lock_time_milliseconds` is 10 min (`server/config.py:111`), published at DI `i=6387` (`server/lock.py:204-207`).
- The lock is released when the owner's session closes (`:209`, `:268-273`).
- `LockingUser` is the user name. `LockingClient` is the session name; the TODO notes it should be the client ApplicationUri (`:233-246`).
- The lock is mandatory on machines and optional on components (`server/mixins/lockable_mixin.py`; client `client/remote_machine.py:51-55`, `client/remote_component.py:424-427`).

**Client** (`client/remote_lock.py`):
- `init_lock` / `break_lock` / `exit_lock` / `renew_lock` each make a plain `ua_node.call_method(<method NodeId>)` with no args and **ignore the returned status integer**. Bad status codes raise asyncua `UaError` subclasses (`:239-278`). A port should decide whether to surface the `-1` results.
- On init, the client browses the 8 child nodes by name (all asserted present, `:315-364`). It reads `MaxInactiveLockTime` from DI i=6387 and falls back to the ctor default of 60 000 ms (`:280-313`). It subscribes to Locked, LockingUser, LockingClient and RemainingLockTime (`:366-379`).
- Cache update (`:414-457`):
  - `locked_since` is the ServerTimestamp of the `Locked` notification, falling back to local `now(UTC)`.
  - `locking_user_changed` and `locked_changed` fire only on a change.
  - Each `RemainingLockTime` update runs `_check_renew_due`.
- `renew_due` logic (`:386-412`):
  - Applies only if `locked_by_us` and a remaining value is known.
  - If remaining > max, the max is raised to remaining.
  - If remaining ≥ `max * renew_threshold`, the throttle is re-armed and nothing fires.
  - Otherwise `renew_due(remaining)` fires, at most once per `renew_retry_seconds` (monotonic clock).
- `read_state(update_cache=True)` reads the 4 values in **one** Read request through `ua_read_values`. A bad status or a null value raises `OpenSmiRuntimeError` (`client/common.py:36-56`). When unlocked, user, client and remaining are normalized to empty/None. The method returns `RemoteLockState` and optionally applies it to the cache (`:459-509`).
- "Locking user" means the user name; `locked_by_us` compares it with `RemoteServer.username` (`:176-182`).
- Priority: the client has no priority API. The priority is visible only as `RemoteUser.data.user_level` (a string, `client/remote_user.py:37-38`).

**AutoLock** (`client/auto_lock.py`):
- `lock(blocking=True)` activates the AutoLock once and connects its handlers to `locking_user_changed` and `renew_due` (`:42-45`).
  - In blocking mode it runs `_lock_once` inline: while active and not `locked_by_us`, call `break_lock()` (if `break_lock=True`) or `init_lock()`, log and sleep `interval` on failure, and break on success (`:83-100`). There is no timeout, so blocking can wait forever.
  - In non-blocking mode it schedules the same loop as a task (`:76-81`).
- `on_locking_user_changed`: if the new user is not us (including `""` on release or expiry), it schedules a re-lock (`:102-106`).
- `on_renew_due`: starts at most one renew task at a time, which calls `renew_lock()` with a 5 s timeout and only logs errors (`:108-120`).
- `unlock()` deactivates, disconnects the handlers, cancels the tasks and calls `exit_lock()` if `locked_by_us`, logging any error (`:52-74`).
- Edge cases (inferred from the code):
  - The loop condition `not locked_by_us` reads the **cached** state, so right after a successful `init_lock` the loop exits through `break`. A failure is retried.
  - Re-locking after a reconnect relies on the `LockingUser` notification.

---

## 7. Subscriptions, notifications, history and security

**SubscriptionManager** (`core/subscription_manager.py:66-442`): one OPC UA subscription is shared for everything ("to stay within max subscription limits").
- `init()` (`:134-154`):
  - Reads `MaxMonitoredItemsPerCall` (fallback 100).
  - Creates the subscription with the requested publishing interval and logs a warning if the server revises it.
  - Subscribes to `CurrentTime` as a keep-alive.
  - Starts a heartbeat task.
- Heartbeat (`:390-399`): every `heartbeat_stale_after/4` (default 10/4 s) the task checks the time since the last notification. Past 10 s it deletes the subscription, then recreates it and re-adds all data and event items in a 1 s retry loop (`:196-211`). Failed renewals leave `handle=None`, and per the TODO they are not retried individually (`:168`, `:179-183`).
- `subscribe_data_change(handler, nodes)` and `subscribe_data_change_batch([(handler, nodes)…])` deduplicate per NodeId. A handler is attached once. Items are created in chunks of `max_items_per_call`. The methods return `{Node: StatusCode}` for failures; the client never checks this return value (`:301-325`, `:244-255`).
- `subscribe_event(handler, source_node)` creates one event monitored item per node and adds handlers to it (`:327-347`). `unsubscribe_data_change` and `unsubscribe_event` remove the item when its last handler is gone (`:349-388`). The client never unsubscribes.
- Dispatch: `datachange_notification` calls each handler's `ua_on_data_change(node, val, data)` and logs and swallows exceptions (`:404-421`). `event_notification` routes by **`ua_event.SourceNode`** to the handlers registered for that NodeId (`:427-442`).
- `shutdown()` and `clear_subscriptions()` (`:156-166`, `:232-237`). `no_of_monitored_items` (`:239-242`).

**What the client subscribes to**:

| Subscription | Where |
|---|---|
| CurrentTime | `client/remote_server.py:390-394` |
| Component `MachineryItemState/CurrentState` | `client/remote_component.py:256-259` |
| Every variable of the 4 component containers | `client/remote_component.py:366-370` |
| Every variable of the 3 callable containers | `client/remote_callable.py:91-94` |
| Skill `StateMachine/CurrentState` | `client/remote_skill.py:107-109` |
| The 4 lock variables | `client/remote_lock.py:373-379` |
| User `IsPresent` | `client/remote_user.py:76-78` |
| Events on each component's `Notification` node | `client/remote_component.py:285-294` |

Users react through the Signals in §2. There is no per-subscription publishing interval or sampling interval option.

**Notifications / events**:
- The server emits `BaseEventType` (info) and MachineSet `AlertType` (alerts, which carry an `ErrorCode`). The emitting node is the `Notification` object (`server/notification.py:41-52`).
- Alert messages are sent as `"<msg>|||<code>"` as an asyncua work-around (`server/notification.py:77-94`).
- The client splits on `|||`. It prefers `ua_event.ErrorCode` and falls back to the split suffix, then to `""`. It emits `NotificationEvent(message, severity, error_code)` (`client/remote_component.py:372-392`). Events with a `None` Message.Text are dropped with a warning.
- Components without their own Notification node forward notifications to their parent on the server side (`server/mixins/notification_mixins.py:7-11`).
- Known issue: the TODO says `emitting_node` is always the Server (i=2253), so routing uses SourceNode instead (`core/subscription_manager.py:431-433`).

**History**:
- `RemoteVariable.read_history(start_time, end_time, num_values=0 (0 = unlimited), return_bounds=True)` performs asyncua `read_raw_history`. It returns `TimestampedValue(value, ServerTimestamp)` and skips entries without a value (`client/remote_variable.py:302-322`).
- On the server, history exists only where it was enabled: `historize=True` on container variables (`server/mixins/ua_variable_container_mixins.py:86-98` etc.) and on the lock's LockingUser/LockingClient (count 1000, `server/lock.py:193-195`). Event history is not implemented (`server/notification.py:54-55`).
- Example: `osc/examples/history.py:17-22`.

**Security policy** (`client/remote_server.py:325-349`):
- `security_policy: asyncua.ua.SecurityPolicyType`. With `NoSecurity` nothing is configured.
- Otherwise, on **every connect**, the client generates a fresh private key and a self-signed certificate: CN "OpenSMI Python Client", SAN = application_uri, EKU serverAuth+clientAuth, valid 3650 days. It then calls `set_security(policy, cert, key, mode)` using `SECURITY_POLICY_TYPE_MAP`.
- There is no option to load a certificate from a file (TODO `:329`), and no server-certificate pinning or trust list is configured. Whether asyncua validates the server certificate in this mode was not verified.
- Authentication is always username/password.
- On the server side, NoSecurity is disabled when `config.encryption` is set (`server/_server.py:266-295`).
- Example: `Aes256Sha256RsaPss_SignAndEncrypt` (`osc/examples/security_policy.py:19-24`).

---

## 8. Errors and exceptions

**Core hierarchy** (`core/errors.py:8-67`):

```
Exception
├── OpenSmiError
│   ├── SkillNotSuspendableError        – suspend() on a non-suspendable skill
│   ├── OpenSmiRuntimeError(msg, error_code="")   (attrs .msg, .error_code; note: does not call super().__init__)
│   │   ├── SkillHaltedError            – skill halted during a wait
│   │   └── OutOfRangeError             – (server-side use)
│   ├── NoValidValueReadError           – (server-side use)
│   ├── NamespaceUninitializedError     – (server-side use)
│   └── RequiredVariableMissingError    – (server-side use)
├── CycleError(path)                    – lifecycle ordering cycle
ValueError
└── ValidationError                     – server init validation
```

**Raised by the client**:

| Exception | When | Source |
|---|---|---|
| `RuntimeError` | `connect()` while already connecting or not DISCONNECTED; `RemoteVariable.write_value` when not writable or the value is invalid; `ua_node` accessed before set / set twice | `client/remote_server.py:196-201`; `client/remote_variable.py:347-367`; `core/base_ua_object.py:59-76` |
| `ConnectionError` | max retries exceeded | `client/remote_server.py:258-260` |
| `ConnectionRefusedError` | wrong credentials (`BadUserAccessDenied` / `BadIdentityTokenInvalid`) | `:246-250` |
| `ConnectionAbortedError` | `disconnect()` during `connect()` | `:213-217` |
| `OpenSmiRuntimeError` | `read_state` with a bad status or null value | `client/common.py:36-48` |
| `SkillHaltedError` | halted while waiting in start/reset/suspend/wait_for_state | `client/remote_skill.py:204-215` |
| `SkillNotSuspendableError` | suspend without a Suspend node | `:177-189` |
| `KeyError` | container `[name]` / `write_all` with an unknown name; `machines[...]`; `ua_get_namespace_index` with an unknown URI; `read_current_state` with an unknown state text | `client/remote_variable_container.py:85-94`; `core/base_server.py:57-65`; `client/remote_skill.py:54` |
| `ValueError` | RemoteLock setters | `client/remote_lock.py:203-235` |
| `AssertionError` | mandatory child nodes missing (lock: 8 nodes; skill: CurrentState/Start/Halt/Reset); `RemoteMachine.lock` when not browsed | `client/remote_lock.py:346-353`; `client/remote_skill.py:119-122`; `client/remote_machine.py:54` |
| asyncua `UaError` / `UaStatusCodeError` subclasses (e.g. `BadInvalidState`, `BadUserAccessDenied`, `BadRequiresLock`, `BadLocked`, `BadOutOfRange`, `BadNoMatch`) | passed through from method calls, reads and writes | e.g. `client/remote_lock.py:245`, `client/remote_method.py:50-56` |

**Swallowed or collected** (important for a port):
- `common.gather` collects ordinary `Exception`s from concurrent child browses into `init_exceptions` (EXPERIMENTAL) on the server and on each object. Only connection/timeout errors and BaseExceptions propagate (`client/common.py:16-33`). A partially failed browse is silent apart from logs and `init_exceptions`.
- Signal-handler exceptions on data change or event are logged and dropped by the SubscriptionManager (`core/subscription_manager.py:418-421`, `:439-442`).
- A failure to load custom data types only produces a warning (`client/remote_server.py:358-364`). A failed `MaxInactiveLockTime` read falls back to the default (`client/remote_lock.py:302-313`).

---

## 9. Examples (`osc/examples/`)

| File | Feature shown |
|---|---|
| `quick_start.py` | `async with RemoteServer`, `machine.lock.init_lock()`, `read_current_state`, `reset(wait)`, `parameter_set.write_all`, `start(wait)`, `final_result_data.read_all` against `ExampleMachine` on port 4841 (`:17-36`) |
| `example.py` | iterate `skill_set`, connect `state_changed`, lock, bring up the `StartupSkill` (reset/start), then run `DummySkill` with parameters and results (`:24-49`) |
| `method_call.py` | lock, write `method.parameter_set`, `method.call()`, read `final_result_data` (`:18-30`) |
| `auto_lock.py` | `AutoLock(machine.lock).lock()` held until Ctrl+C (`:14-21`) |
| `browse_rules.py` | custom `browse_when` rules (`match_all` + MACHINES/MONITORING; `match_name("DummyMachine")` + COMPONENTS/SKILL_SET) and `dto()` output (`:17-39`) |
| `direct_component.py` | `browse_rules=()` plus manual `RemoteComponent(...ua_node=get_node("ns=1;s=DummyMachine.Components.DummyComponent")).init()` (direct node-id mode) (`:18-33`) |
| `dto.py` | `machine.dto()` serialized to JSON via `dataclasses.asdict` with a custom serializer (`:16-32`) |
| `history.py` | `RemoteVariable.read_history(num_values=10)` on a monitoring variable (`:17-22`) |
| `security_policy.py` | connect with `SecurityPolicyType.Aes256Sha256RsaPss_SignAndEncrypt`, plus `setup_logging` (`:19-31`) |
| `subscription_component.py` | `notification_received` and `status_changed` signals on a machine and a component (`:15-24`) |
| `subscription_server.py` | `RemoteServer.status_changed` and `time_updated` signals (`:21-32`) |
| `subscription_variable.py` | `value_changed` on every variable of a component's `monitoring` (`:13-21`) |

Dummy server credentials used in the examples: `visitor`, `operator` and `orchestrator` (same string as the password) on `opc.tcp://localhost:4843/`.

---

## Appendix: other core modules
- `core/ua.py:12-17`: `NodeIdDefinition(ns_uri, id: Int32)`, an unresolved NodeId resolved through `BaseServer.ua_get_node_id`.
- `core/config.py:17-92`: TOML-to-dataclass loader (`load_toml`, `load_dataclass`, `from_dict`), used by the server config. The client does not use it.
- `core/ua_enums.py:63-93`: `MachineryItemState` (NotAvailable, OutOfService, NotExecuting, Executing) and `MachineryOperationMode` (None, Maintenance, Setup, Processing), both LocalizedText enums. The client exposes the component state only as a raw `str`, not as this enum.
- `core/unit_all.py`: auto-generated `Unit(BaseUnit)` enum with about 2121 UN/CEFACT Rec20 units, each `(symbol, ua_unit_id)` (header `:1-12`). `BaseUnit` (`core/unit_base.py:13-69`) provides `symbol`, `from_ua_unit_id`, `from_symbol`, `ua_unit_id` and `ua_eu_information`. The client does not use `Unit`; `RemoteVariable.unit` is the raw `EUInformation`.
- `core/ua_node_util.py`: `get_children_browse_names` (one Browse of the children → `{Node: QualifiedName}`, `:252-259`), `get_child_without_ns` (`:181-204`), `get_type_definition` (`:131-146`), `get_properties` (`:101-128`), `write_value` (`:62-82`), `get_node_id` (`:149-178`), plus `read_unit`/`write_unit`/`write_range` (server-oriented).
