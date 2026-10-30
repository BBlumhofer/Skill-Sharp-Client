# Spezifikation: OpenSMI-Client für C# (`OpenSmi.Client`)

Stand: 2026-10-29. Referenz-Snapshots: `SmartFactory-KL/opensmi-client` @ `e8740c2` (2026-10-23),
`opensmi-core` @ `68ef623`, `opensmi-server` @ `29e4d4a`, `Skill-Sharp-Client` @ `e57b3c0` (2026-02-13).
Die Detail-Inventare mit `file:line`-Belegen liegen in [`analyse/`](analyse/). Diese Spec verweist
auf sie mit **[OI §x]** (OpenSMI-Inventar) und **[SI §x]** (Skill-Sharp-Inventar).

## 1. Ziel und Abgrenzung

**Ziel:** Eine C#-Bibliothek, die sich gegenüber einem SMI-konformen OPC-UA-Server (z. B.
`opensmi-server`) so verhält wie der Python-Client `opensmi-client`. Sie bildet dasselbe
Objektmodell, dieselbe Browse-Logik, dieselbe Skill- und Lock-Semantik und dieselben DTOs ab, mit
einer API, die sich in C# natürlich anfühlt.

**Nicht-Ziele:**
- Kein 1:1-Transkript von Python. Wo Python-Idiome (Context-Manager, `asyncio.timeout`, Signals per
  `set`) keine natürliche Entsprechung haben, gilt die C#-Konvention (§3).
- Keine Server-Implementierung, kein Port von `opensmi-core` jenseits dessen, was der Client braucht.
- SmartFactory-spezifische Funktionen (Ports, Storages, RFID-Nachbarn, Startup/Recovery) gehören
  **nicht** in den Kern. Sie kommen in ein optionales Erweiterungspaket (§9).
- Keine API-Kompatibilität zum alten `SkillSharp.Client`. IMAPRO hängt nicht mehr davon ab
  [SI §7].

**Bewusste Abweichungen vom Python-Client** (dort Bugs oder Unschärfen, [OI Zusammenfassung]):
siehe §10.

## 2. Pakete und Projektstruktur

| Projekt | Inhalt | Abhängigkeiten |
|---|---|---|
| `src/OpenSmi.Client` | Kern: Server, Machine, Component, Resource, Skill, Method, Variable, Lock, AutoLock, User, Browse-Regeln, DTOs, Subscriptions | `OPCFoundation.NetStandard.Opc.Ua.Client` (1.5.377.x), `Microsoft.Extensions.Logging.Abstractions` |
| `src/OpenSmi.Client.SmartFactory` (optional, Phase 6) | Ports, Storages, Nachbarn, Startup/Recovery auf Basis des Kerns | `OpenSmi.Client` |
| `tests/OpenSmi.Client.Tests` | Unit-Tests ohne Server | xUnit |
| `tests/OpenSmi.Client.IntegrationTests` | Tests gegen einen lokal gestarteten `opensmi-server` (Python) | xUnit, Python ≥ 3.11 im CI |
| `examples/` | C#-Pendants zu `opensmi-client/examples/*.py` | `OpenSmi.Client` |

- Target Framework: `net10.0`. `net8.0` als zusätzliches Target ist eine offene Entscheidung (§11).
- Root-Namespace = Assembly-Name = PackageId: `OpenSmi.Client`. Damit endet der Wildwuchs aus
  `UAClient.*` und `SkillSharp.Client` [SI §1].
- Lizenz: MIT, wie upstream. Bei übernommenen Algorithmen gehört der upstream-Copyright-Hinweis
  in die `NOTICE`.

## 3. Querschnitts-Konventionen

| Thema | Python-Original | C#-Festlegung |
|---|---|---|
| Asynchronität | `async def` | `Task`/`ValueTask`, jede öffentliche I/O-Methode hat einen `CancellationToken cancellationToken = default` als letzten Parameter |
| Timeouts | `asyncio.timeout()` beim Aufrufer | über `CancellationToken`. Zusätzlich `TimeSpan? timeout` auf `Wait*`-Methoden, weil die Python-Waits ohne Timeout hängen können [OI §5] |
| Lebenszyklus | `await obj.init()`, `RemoteServer` als `async with` | `InitAsync(ct)` (idempotent), `RemoteServer : IAsyncDisposable` (`await using`) |
| Signals | `Signal[P]` mit async-Callbacks, `gather` | `event AsyncEventHandler<XxxEventArgs>` (Handler geben `Task` zurück), dazu `Watch…Async(ct)` als `IAsyncEnumerable` und die `WaitFor…Async`-Methoden. Handler laufen nacheinander aus der seriellen Dispatch-Schleife, Ausnahmen werden pro Handler geloggt. Entscheidung: Ticket 25/27 in `.scratch/opensmi-port` |
| Logging | structlog | `ILogger<T>` über `ILoggerFactory` im `RemoteServerOptions`, Standard `NullLoggerFactory` |
| Fehler | asyncua-Exceptions plus `OpenSmiError` | eigene Hierarchie (§8). OPC-UA-Statusfehler als `OpenSmiStatusException` mit `StatusCode`, keine rohen SDK-Exceptions an der Oberfläche |
| Collections | `dict[str, X]` | `IReadOnlyDictionary<string, X>`, case-sensitive wie BrowseNames. Schreibzugriff nur intern, thread-safe aufgebaut |
| Nebenläufigkeit | asyncio single-threaded | Notifications kommen auf SDK-Threads. Cache-Felder sind `volatile` oder lock-geschützt; Signals werden nicht auf dem SDK-Publish-Thread awaitet, sondern über einen `Channel<T>` je Server seriell abgearbeitet (Reihenfolge je Node bleibt erhalten) |
| Konfiguration | Konstruktor-Kwargs | `RemoteServerOptions` (record) |

## 4. Objektmodell (öffentliche API)

Die Signaturen sind normativ in Namen und Semantik, nicht in jedem Parameter-Detail. Pro Klasse
gilt die Python-Semantik aus [OI §2], soweit hier nichts anderes steht.

### 4.1 `RemoteServer : IAsyncDisposable` [OI §2.2]

```csharp
public sealed record RemoteServerOptions
{
    public required string Url { get; init; }
    public required string Username { get; init; }
    public required string Password { get; init; }
    public string? ApplicationUri { get; init; }               // Default: urn:uuid:<guid>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan WatchdogInterval { get; init; } = TimeSpan.FromSeconds(1);
    public bool AutoReconnect { get; init; } = true;
    public TimeSpan ReconnectMaxDelay { get; init; } = TimeSpan.FromSeconds(30);
    public SecurityOptions Security { get; init; } = SecurityOptions.None;    // §7.3
    public IReadOnlyList<BrowseRule>? BrowseRules { get; init; }              // null = alles, leer = nichts
    public TimeSpan PublishingInterval { get; init; } = TimeSpan.FromMilliseconds(100);
    public ILoggerFactory? LoggerFactory { get; init; }
}

public sealed class RemoteServer : IAsyncDisposable
{
    public RemoteServer(RemoteServerOptions options);
    public Task<RemoteServer> ConnectAsync(ConnectRetryOptions? retry = null, CancellationToken ct = default);
    public Task DisconnectAsync(CancellationToken ct = default);

    public RemoteServerStatus Status { get; }          // Disconnected, Connecting, Browsing, Connected, Reconnecting, Disconnecting
    public bool IsConnected { get; }
    public IReadOnlyDictionary<string, RemoteMachine> Machines { get; }
    public string Username { get; }
    public IReadOnlyList<Exception> InitExceptions { get; }     // §8.2
    public ISession? Session { get; }                           // Escape-Hatch, entspricht ua_client

    public event AsyncEventHandler<ServerStatusChangedEventArgs>? StatusChanged;
    public event AsyncEventHandler<ServerTimeUpdatedEventArgs>? TimeUpdated;
    public IAsyncEnumerable<RemoteServerStatus> WatchStatusAsync(CancellationToken ct = default);

    public RemoteUaObject? GetUaObject(NodeId nodeId);
    public ushort GetNamespaceIndex(string namespaceUri);       // KeyNotFoundException
}
```

- Ablauf von `ConnectAsync` wie [OI §2.2]. Retry mit exponentiellem Backoff
  (`ConnectRetryOptions { MaxRetries = 3, MinDelay = 0.5 s, MaxDelay = 30 s }`, `MaxRetries = null`
  heißt unendlich). `BadIdentityTokenRejected` wird erneut versucht.
  `BadUserAccessDenied`/`BadIdentityTokenInvalid` → `OpenSmiAuthenticationException`
  (entspricht `ConnectionRefusedError`).
- Nur **ein** Reconnect-Mechanismus: der `SessionReconnectHandler` des SDK. Der zweite
  Polling-Loop aus SSC entfällt [SI §2.2].
- **Nach einem Reconnect bleiben die Objekt-Identitäten erhalten.** Es wird nicht neu gebrowst,
  der SubscriptionManager stellt die Items wieder her (wie Python). Bei SSC entstanden neue
  `RemoteModule`-Instanzen, das ist ausdrücklich verboten.
- `DisposeAsync` ruft `DisconnectAsync`.

### 4.2 `RemoteUaObject` (abstrakte Basis) [OI §2.0]

`Name`, `Path` (POSIX-Pfad als `string`, z. B. `/M/C/Monitoring/Var`), `NodeId`, `UaType`
(BrowseName der TypeDefinition, Default `"UNKNOWN"`), `Parent`, `Server`, `BrowseRules`,
`InitExceptions`, `LifecycleState`, `InitAsync(ct)`, `ReadUaTypeAsync(ct)`. Beim Setzen der
`NodeId` registriert sich das Objekt im Server-Register (Weak-Referenzen, entspricht
`WeakValueDictionary`).

### 4.3 `BaseRemoteComponent`, `RemoteComponent`, `RemoteMachine`, `RemoteResource` [OI §2.3–2.6]

```csharp
public abstract class BaseRemoteComponent : RemoteUaObject
{
    public IReadOnlyDictionary<string, RemoteSkill> SkillSet { get; }
    public IReadOnlyDictionary<string, RemoteSkill> FeasibilityCheckSet { get; }
    public IReadOnlyDictionary<string, RemoteSkill> PreconditionCheckSet { get; }
    public IReadOnlyDictionary<string, RemoteMethod> MethodSet { get; }
    public IReadOnlyDictionary<string, RemoteComponent> Components { get; }
    public IReadOnlyDictionary<string, RemoteResource> Resources { get; }
    public Identification Identification { get; }
    public Monitoring Monitoring { get; }
    public Attributes Attributes { get; }
    public ParameterSet ParameterSet { get; }
    public string CurrentState { get; }                       // MachineryItemState, Default "Unknown"
    public MachineryItemState? CurrentStateEnum { get; }      // zusätzlich typisiert (Core-Enum)
    public Task<string> ReadCurrentStateAsync(CancellationToken ct = default);
    public event AsyncEventHandler<ComponentStatusChangedEventArgs>? StatusChanged;
    public event AsyncEventHandler<NotificationReceivedEventArgs>? NotificationReceived;
}
public sealed class RemoteComponent : BaseRemoteComponent { public RemoteLock? Lock { get; } public RemoteComponentDto ToDto(); }
public sealed class RemoteMachine   : BaseRemoteComponent { public RemoteLock Lock { get; } public IReadOnlyList<RemoteUser> Users { get; } public RemoteMachineDto ToDto(); }
public sealed class RemoteResource  : BaseRemoteComponent { public RemoteResourceDto ToDto(); }
```

- `RemoteMachine.Lock` wirft `InvalidOperationException` mit klarer Meldung, wenn das Feature
  `Lock` nicht gebrowst wurde. Python wirft hier einen `AssertionError`, und `dto()` stürzt ab.
  `ToDto()` liefert in diesem Fall `Lock = null`.
- `ReadCurrentStateAsync` liefert immer den Text (Python liefert teils das rohe LocalizedText).

### 4.4 `RemoteCallable`, `RemoteMethod`, `RemoteSkill` [OI §2.7–2.9, §5]

```csharp
public abstract class RemoteCallable : RemoteUaObject
{
    public ParameterSet ParameterSet { get; }
    public Monitoring Monitoring { get; }
    public FinalResultData FinalResultData { get; }
    public int? MinAccessLevel { get; }
    public IReadOnlyList<NodeId> DependsOn { get; }
    public IReadOnlyList<NodeId> DependencyOf { get; }
}

public sealed class RemoteMethod : RemoteCallable
{
    // Ruft "Call" auf dem Method-Objekt OHNE Eingangsargumente, liefert danach FinalResultData.ReadAllAsync().
    public Task<IReadOnlyDictionary<string, object?>> CallAsync(CancellationToken ct = default);
}

public sealed class RemoteSkill : RemoteCallable
{
    public SkillState CurrentState { get; }          // gecacht, initial Halted
    public bool IsSuspendable { get; }
    public bool IsFinite { get; }                    // TypeDefinition des Skill-Objekts beginnt mit "Finite"
    public event AsyncEventHandler<SkillStateChangedEventArgs>? StateChanged;
    public IAsyncEnumerable<SkillState> WatchStateAsync(CancellationToken ct = default);

    public Task StartAsync(StartWait wait = StartWait.None, TimeSpan? timeout = null, CancellationToken ct = default);
    public Task ResetAsync(bool wait = false, TimeSpan? timeout = null, CancellationToken ct = default);
    public Task HaltAsync(bool wait = false, TimeSpan? timeout = null, CancellationToken ct = default);
    public Task SuspendAsync(bool wait = false, TimeSpan? timeout = null, CancellationToken ct = default);
    public Task WaitForStateAsync(SkillState target, TimeSpan? timeout = null, CancellationToken ct = default);
    public Task<SkillState> ReadCurrentStateAsync(CancellationToken ct = default);
    public RemoteSkillDto ToDto();
}

public enum StartWait { None, RunningOrCompleted, Completion }
```

- **`NodeId` des Skills ist der `SkillExecution`-Knoten** (bzw. `FeasibilityCheck`/`PreconditionCheck`),
  `Name` ist der BrowseName des Skill-Objekts darüber. `IsFinite` liest die TypeDefinition des
  **Eltern**-Knotens [OI §2.9].
- Die Methoden `Start`/`Halt`/`Reset`/`[Suspend]` werden einmal beim Init unter `StateMachine`
  aufgelöst und gecacht. Der Aufruf geht mit `ObjectId = StateMachine` und ohne Argumente
  [OI §5]. Die SSC-Suche über Teilstrings, BFS und Retry entfällt [SI §2.6].
- Wait-Semantik wie Python (Halt-Zähler vor dem Call erfassen, auf den gecachten Zustand warten,
  `SkillHaltedException`, wenn Halted kein Ziel ist), mit zwei Ergänzungen:
  - `StartWait.Completion` wartet bei finiten Skills auf `Completed`. Python wartet nur auf
    `Running|Completed`, obwohl die README „wait for completion“ verspricht [OI §5]. Bei
    kontinuierlichen Skills wirft `Completion` eine `InvalidOperationException`.
  - Nach Ablauf von `timeout` kommt eine `TimeoutException`. Ohne Timeout und Token wartet der
    Aufruf, wie in Python, unbegrenzt.
- Unbekannter Zustandstext: `ReadCurrentStateAsync` wirft `OpenSmiProtocolException` statt
  `KeyError`. Im Subscription-Pfad wird er geloggt und ignoriert. Ein leerer Text bedeutet
  `Halted` (wie Python).
- `SuspendAsync` ohne Suspend-Knoten wirft `SkillNotSuspendableException`.

### 4.5 `RemoteVariable`, Variable-Container [OI §2.10–2.11]

```csharp
public sealed class RemoteVariable : RemoteUaObject
{
    public object? Value { get; }                 // verarbeitet (Enum-Mapping, NodeId → DisplayName-Key …)
    public Variant RawValue { get; }
    public DateTime? Timestamp { get; }           // ServerTimestamp
    public bool IsWritable { get; }               // aus UserAccessLevel (CurrentWrite), NICHT aus dem Ordnernamen
    public string UaDataType { get; }
    public int UaValueRank { get; }               // korrekt aus dem Read; in Python immer 0
    public IReadOnlyDictionary<string, string> ValidValues { get; }
    public EUInformation? Unit { get; }
    public Opc.Ua.Range? Range { get; }
    public event AsyncEventHandler<VariableValueChangedEventArgs>? ValueChanged;
    public IAsyncEnumerable<Variant> WatchValueAsync(CancellationToken ct = default);

    public Task<object?> ReadValueAsync(bool updateCache = false, CancellationToken ct = default);
    public Task<Variant> ReadRawValueAsync(CancellationToken ct = default);
    public Task<string?> ReadResolvedValueAsync(CancellationToken ct = default);
    public Task WriteValueAsync(object? value, CancellationToken ct = default);
    public Task<IReadOnlyList<TimestampedValue>> ReadHistoryAsync(DateTime? start = null, DateTime? end = null,
                                                                   uint numValues = 0, bool returnBounds = true,
                                                                   CancellationToken ct = default);
    public RemoteVariableDto ToDto();
}

public abstract class RemoteVariableContainer : RemoteUaObject, IReadOnlyDictionary<string, RemoteVariable>
{
    public Task<IReadOnlyDictionary<string, object?>> ReadAllAsync(CancellationToken ct = default);   // EIN Read-Request (Batch)
    public Task WriteAllAsync(IReadOnlyDictionary<string, object?> values, CancellationToken ct = default); // EIN Write-Request
}
public sealed class Attributes, Identification, Monitoring, ParameterSet, FinalResultData : RemoteVariableContainer;
```

- **Typkonvertierung beim Schreiben:** Der Ziel-`BuiltInType` kommt aus dem gecachten
  `RawValue.TypeInfo` bzw. dem DataType. `int` → `Double` usw. wird konvertiert. In SSC fehlte das,
  Folge war `BadTypeMismatch` [SI §2.11]. `string` → `NodeId` und `string` → `LocalizedText(en-US)`
  wie in `ua_node_util.write_value` [OI §2.10].
- Bei abgelehntem Wert gilt die Fallback-Logik über `ValidValues`-Displaynamen wie in Python.
  Danach kommt `OpenSmiInvalidValueException`.
- `ReadAllAsync`/`WriteAllAsync` arbeiten **gebatcht**: ein Request statt N sequenzieller
  Requests wie in Python. Bei `WriteAllAsync` mit unbekanntem Namen fliegt
  `KeyNotFoundException`, bevor überhaupt geschrieben wird.
- Variablen mit derselben NodeId werden über das Server-Register wiederverwendet.

### 4.6 `RemoteLock`, `AutoLock` [OI §2.12–2.13, §6]

```csharp
public sealed class RemoteLock : RemoteUaObject
{
    public bool IsLocked { get; }
    public string LockingUser { get; }
    public string LockingClient { get; }
    public DateTime? LockedSince { get; }
    public bool IsLockedByUs { get; }             // IsLocked && LockingUser == Server.Username
    public int? RemainingLockTimeMs { get; }
    public int MaxInactiveLockTimeMs { get; set; }    // > 0
    public double RenewThreshold { get; set; }        // 0 < x < 1, Default 0.5
    public TimeSpan RenewRetryInterval { get; set; }  // > 0, Default 1 s

    public Task<LockResult> InitLockAsync(CancellationToken ct = default);
    public Task<LockResult> BreakLockAsync(CancellationToken ct = default);
    public Task<LockResult> ExitLockAsync(CancellationToken ct = default);
    public Task<LockResult> RenewLockAsync(CancellationToken ct = default);
    public Task<RemoteLockState> ReadStateAsync(bool updateCache = true, CancellationToken ct = default);

    public event AsyncEventHandler<LockedChangedEventArgs>? LockedChanged;
    public event AsyncEventHandler<LockingUserChangedEventArgs>? LockingUserChanged;
    public event AsyncEventHandler<RenewDueEventArgs>? RenewDue;
}

public enum LockResult { Ok = 0, NotLocked = -1 }   // Rückgabewerte des Servers; Bad-Status → Exception

public sealed class AutoLock : IAsyncDisposable
{
    public AutoLock(RemoteLock remoteLock, bool breakLock = false, TimeSpan? interval = null);
    public Task LockAsync(bool blocking = true, CancellationToken ct = default);
    public Task UnlockAsync(CancellationToken ct = default);
    public bool IsActive { get; }
}
```

- **Eigentümerschaft** wird ausschließlich über `LockingUser == Username` bestimmt, wie in Python.
  Die SSC-Heuristik (ApplicationName, Hostname, IP, localhost, „unlesbar = gehört uns“) ist
  falsch und fällt weg [SI §2.3].
- Methoden werden exakt per BrowseName aufgelöst (`InitLock`, `BreakLock`, `ExitLock`,
  `RenewLock`). Kein Teilstring-Fallback und **kein** Durchprobieren weiterer Methoden. In SSC
  konnte `InitLock` dadurch in `ExitLock` laufen [SI §2.9].
- Der Integer-Rückgabewert des Servers wird ausgewertet und als `LockResult` geliefert. Python
  ignoriert ihn [OI §6].
- `InitLock` wird ohne Argumente aufgerufen, so wie es der opensmi-server erwartet
  ([OI §6]: optionales `context`). Ein optionaler `context`-Parameter ist vorgesehen, falls
  DI-konforme Fremdserver ihn verlangen.
- `MaxInactiveLockTime` wird aus DI `i=6387` gelesen, Fallback 60 000 ms.
- `AutoLock`: Verhalten wie [OI §6] (bei Verlust neu sperren, bei `RenewDue` erneuern, ein
  Renew-Task gleichzeitig, Renew-Timeout 5 s). `blocking=true` respektiert `ct`.
  `DisposeAsync` ruft `UnlockAsync`.

### 4.7 `RemoteUser` [OI §2.14]

`Name`, `Data` (`AllowMultiple`, `UserLevel`, `MaxAccessLevel`, `IsPresent`),
`UserPresenceChanged`. Im DTO heißt das Feld **`LockPriority`** (aus `UserLevel` geparst,
`null` wenn nicht numerisch), nicht `current_access_level` wie in Python. Dort ist das ein
Benennungsfehler [OI §2.14].

### 4.8 `NotificationEvent` [OI §2.15, §7]

`record NotificationEvent(string Message, int Severity, string ErrorCode)`. Das Parsing von
`"<msg>|||<code>"` erfolgt wie in Python, `ErrorCode` aus dem Event hat Vorrang. Fallback auf
den Kindknoten `Messages`, wenn `Notification` `BadServiceUnsupported` liefert.

## 5. Browsing [OI §3]

- Einstieg: Machinery `Machines` (`nsu=http://opcfoundation.org/UA/Machinery/;i=1001`). Fallback:
  erstes Kind von `Objects` mit `BrowseName.Name == "Machines"`, unabhängig vom Namespace. Die
  SSC-Fallbacks `SFM:ModuleSet` und „DisplayName enthält Module/Machine“ gehören in das
  SmartFactory-Paket, nicht in den Kern.
- Dispatch der Kinder per **exaktem** `BrowseName.Name` gemäß Tabelle [OI §3]: `Lock`, `SkillSet`,
  `Notification`, `Identification`, `Attributes`, `Monitoring`, `ParameterSet`, `Components`,
  `Resources`, `MachineryBuildingBlocks`, `MethodSet`. Unbekannte Kinder erzeugen nur eine
  Warnung im Log. **Keine** Teilstring- oder TypeDefinition-Heuristiken wie in SSC [SI §4].
- Browse erfolgt **rekursiv** (Komponenten in Komponenten). Die SSC-Discovery ging nur eine Ebene
  tief [SI §2.3].
- Pfadmodell und Regelauswertung exakt wie [OI §3]: Ordnersegmente (`Components`, `SkillSet`,
  `MethodSet`) erscheinen nicht im Pfad.
- `BrowseFeature` als `[Flags] enum`, `BrowseRule(Func<string, bool> Match, BrowseFeature Features)`,
  Helfer `BrowseRules.When(match, params BrowseFeature[])`, `Match.All/Name/Path/PathPrefix/Any/AllOf/Not`,
  `BrowseRule.Default`.
- `BrowseRules = []` überspringt die Discovery. Danach können Objekte direkt aus bekannten NodeIds
  erzeugt werden (Direct-Node-ID-Modus):
  `await RemoteComponent.CreateAsync(server, name, nodeId, parent: null, rules, ct)`.
- Kinder-Browses eines Objekts laufen nebenläufig (begrenzt, Default 8). Fehler einzelner Kinder
  landen in `InitExceptions` und sind zusätzlich über die Option `ThrowOnPartialBrowse` (Default
  `false`) als `AggregateException` abrufbar. Verbindungs- und Timeout-Fehler brechen ab.
- Browse- und Read-Operationen arbeiten gebatcht (`BrowseAsync` mit mehreren Knoten,
  `ReadAsync` mit mehreren Attributen). Dafür gibt es keinen statischen, unbegrenzten
  Path-Cache wie in SSC [SI §2.14].

## 6. Informationsmodell (normativ) [OI §4]

Erwartete Struktur unterhalb von `Objects/Machines`, siehe den Baum in [OI §4.2]. Kernpunkte:

| Element | Pfad / Kennung |
|---|---|
| Maschinenordner | Machinery `i=1001` |
| Lock | `<Machine>/Lock` (DI `LockingServicesType`), Variablen `Locked`, `LockingUser`, `LockingClient`, `RemainingLockTime`, Methoden `InitLock`, `ExitLock`, `BreakLock`, `RenewLock`; `MaxInactiveLockTime` = DI `i=6387` |
| Komponentenzustand | `MachineryBuildingBlocks/MachineryItemState/CurrentState` (LocalizedText) |
| Users | `MachineryBuildingBlocks/Users/<User>/{AllowMultiple, UserLevel, MaxAccessLevel, IsPresent}` |
| Skill | `SkillSet/<Skill>/SkillExecution` (bzw. `FeasibilityCheck`, `PreconditionCheck`) |
| Skill-Zustandsmaschine | `…/SkillExecution/StateMachine/{CurrentState, Start, Halt, Reset, [Suspend]}` |
| Skill-Container | `…/SkillExecution/{ParameterSet, Monitoring, FinalResultData, MinAccessLevel, Requirements}` |
| Methode | `MethodSet/<Method>/{Call, ParameterSet, Monitoring, FinalResultData, MinAccessLevel, Requirements}` |
| Abhängigkeiten | Referenztyp `Requires` (vorwärts = `DependsOn`, invers = `DependencyOf`) |
| Variablen-Metadaten | Properties `EngineeringUnits`, `EURange`; NodeId-Werte mit `Utilizes`-Referenzen ergeben `ValidValues` |

**Offen:** Ob `Requirements` am Skill-Objekt oder unter `SkillExecution` liegt, ist nicht
verifiziert [OI §4.2]. Der Client sucht an beiden Stellen und nimmt den ersten Treffer. Ein
Integrationstest entscheidet die Frage (Plan, Phase 1).

`SkillState`-IDs 11–20 und `MachineryItemState`/`MachineryOperationMode` werden als C#-Enums aus
`opensmi-core/ua_enums.py` übernommen. `Unit` (Rec-20-Tabelle, ca. 2100 Einträge) wird **nicht**
von Hand portiert, sondern bei Bedarf per Generator aus `unit_all.py` erzeugt. Der Kern braucht
sie nicht, weil `Unit` als `EUInformation` exponiert wird.

## 7. Subscriptions, History, Security

### 7.1 SubscriptionManager [OI §7]
- **Eine** gemeinsame Subscription pro Server. Items werden je NodeId dedupliziert, ein Item kann
  mehrere Handler tragen. Das Anlegen erfolgt in Chunks von `MaxMonitoredItemsPerCall` (Fallback
  100).
- Die Notification **verwendet den gelieferten Wert**. Die SSC-Praxis „bei jeder Änderung neu
  lesen“ ist verboten [SI §2.13].
- Heartbeat über `CurrentTime`: Kommt länger als `HeartbeatStaleAfter` (Default 10 s) keine
  Notification, wird die Subscription gelöscht und mit allen Data- **und** Event-Items neu
  angelegt (SSC verlor Event-Items beim Rebind). Fehlgeschlagene Einzel-Items werden mit Backoff
  erneut versucht; Python versucht das nicht.
- Events werden per `SourceNode` an die registrierten Handler geroutet. Der EventFilter wählt
  `Message`, `Severity`, `SourceNode` und `ErrorCode`.
- `SubscribeDataChange`/`SubscribeEvent` liefern ein `IAsyncDisposable`-Handle zum Abbestellen.

### 7.2 History
`RemoteVariable.ReadHistoryAsync` über `HistoryRead` (ReadRawModifiedDetails), Semantik wie
[OI §7]. Einträge ohne Wert werden übersprungen.

### 7.3 Security [OI §7]
```csharp
public sealed record SecurityOptions(
    string SecurityPolicyUri, MessageSecurityMode Mode,
    string? CertificateStorePath,           // persistentes Zertifikat (statt pro Connect neu, wie in Python)
    bool AutoAcceptServerCertificate = false,
    string? TrustedPeerStorePath = null)
{ public static SecurityOptions None { get; } }
```
- Ohne Security keine Zertifikatserzeugung.
- Mit Security wird ein selbstsigniertes Zertifikat **einmal** erzeugt und im Store abgelegt
  (CN „OpenSMI C# Client“, SAN = ApplicationUri). Python erzeugt es bei jedem Connect neu.
- Server-Zertifikate werden standardmäßig gegen den Trust-Store geprüft. `AutoAccept` muss man
  explizit einschalten. SSC akzeptierte hart kodiert alles [SI §2.1].
- Das Session-Objekt wird über `DefaultSessionFactory`/`ISessionFactory` erzeugt, **ohne
  Reflection** [SI §2.1].
- Authentifizierung über Username/Password (wie upstream).

## 8. Fehlerbehandlung

### 8.1 Hierarchie
```
OpenSmiException
├── OpenSmiStatusException (StatusCode)              – Bad-Status aus Read/Write/Call
│   ├── OpenSmiAccessDeniedException                 – BadUserAccessDenied, BadRequiresLock
│   └── OpenSmiInvalidStateException                 – BadInvalidState (z. B. Start im falschen Zustand)
├── OpenSmiAuthenticationException                   – Login abgelehnt
├── OpenSmiConnectionException                       – max. Retries erreicht, Verbindung abgebrochen
├── OpenSmiProtocolException                         – Pflichtknoten fehlt, unbekannter Zustandstext
├── OpenSmiInvalidValueException                     – Schreibwert ungültig / nicht beschreibbar
├── SkillHaltedException                             – Skill während eines Waits gehaltet
└── SkillNotSuspendableException
```
`TimeoutException` und `OperationCanceledException` bleiben die Standardtypen.

### 8.2 Grundsätze
- **Keine leeren `catch {}`**. Jede abgefangene Ausnahme wird geloggt und entweder gesammelt
  (`InitExceptions`) oder weitergereicht. SSC enthält Hunderte stiller Catches [SI §2].
- Fehlende Pflichtknoten (Lock: 8 Knoten; Skill: `CurrentState`, `Start`, `Halt`, `Reset`)
  führen zu `OpenSmiProtocolException` mit Pfad, nicht zu `AssertionError`.
- Exceptions aus Signal-Handlern werden geloggt und erreichen den Sender nicht.

## 9. SmartFactory-Erweiterung (`OpenSmi.Client.SmartFactory`, optional)

Nur nötig, wenn reale SmartFactory-KL-Module (CA-Module & Co.) angesteuert werden sollen. Aufbau
auf dem Kern, nicht als Fork:

| SSC-Funktion [SI §3.6] | Neue Form |
|---|---|
| `RemotePort` (RFID, Magnete, `CoupleSkill`) | `SmartFactoryPort` als Wrapper um `RemoteComponent` mit TypeDefinition `PortType` |
| `RemoteStorage`/`RemoteStorageSlot` | `SmartFactoryStorage`/`StorageSlot` als Wrapper um `RemoteComponent` |
| Nachbarn aus `PartnerRfidTag` | `NeighborResolver` (reine Funktion, unit-testbar) |
| Startup-Skill, `MakeReadyAsync`, `IsReady` | `ModuleReadiness` über `RemoteMachine` + `AutoLock` |
| Auto-Recovery (Relock, Couple, Startup neu) | `ModuleRecoveryService`, genau ein Mechanismus (kein Doppel aus Auto-Relock und Auto-Recovery) |
| `SFM:ModuleSet` als Einstieg | `BrowseRules`/Entry-Point-Option im SmartFactory-Paket |
| `ResolveResourceNodeIdsAsync` | `ResourceTypeResolver` |

## 10. Bewusste Abweichungen vom Python-Original (Zusammenfassung)

| Python | C# | Grund |
|---|---|---|
| `start(wait=True)` wartet nur auf Running/Completed | `StartWait.Completion` zusätzlich | README verspricht Abschluss [OI §5] |
| Waits ohne Timeout | `timeout` + `CancellationToken` | Hängen vermeiden |
| `ua_value_rank` immer 0 | korrekt gelesen | Bug [OI §2.10] |
| `RemoteUserDTO.current_access_level` = Lock-Priorität | `LockPriority` | Benennungsfehler [OI §2.14] |
| `RemoteMachine.dto()` → `AssertionError` ohne Lock | `Lock = null` im DTO | Bug [OI §2.5] |
| `read_current_state` → `KeyError` | `OpenSmiProtocolException` | robustere Fehlerklasse |
| Lock-Rückgabewerte ignoriert | `LockResult` | Information nicht verwerfen [OI §6] |
| Zertifikat pro Connect neu, kein Trust-Store | persistentes Zertifikat, Trust-Store | Sicherheit [OI §7] |
| `read_all`/`write_all` sequenziell | gebatcht | Performance |
| `feasibility/precondition_check_set` nicht im DTO | im DTO enthalten | Vollständigkeit |

Jede Abweichung braucht einen Test und einen Eintrag in `docs/opensmi/DEVIATIONS.md`. Wenn
upstream den Fehler behebt, wird das Verhalten angeglichen.

## 11. Offene Entscheidungen

> Alle Punkte sind entschieden, siehe Ticket 03, 25 und 27 in `.scratch/opensmi-port`: Repo bleibt,
> zusätzlich `net8.0`, SmartFactory-Paket danach, async Events statt `AsyncSignal`, `Unit` wird generiert.

1. **Paketname/Repo:** `OpenSmi.Client` im bestehenden Repo `Skill-Sharp-Client`, oder neues Repo
   `opensmi-client-dotnet`? Wäre ein Beitrag an SmartFactory-KL als offizieller C#-Client erwünscht?
2. **Target Frameworks:** nur `net10.0` oder zusätzlich `net8.0` (LTS)?
3. **SmartFactory-Paket:** wird es überhaupt gebraucht (reale Hardware), oder reicht der Kern?
4. **Signals vs. `IObservable<T>`/`IAsyncEnumerable<T>`:** Die Spec legt `AsyncSignal<T>` fest
   (nah an Python). Rx wäre eine zusätzliche Abhängigkeit.
5. **`Unit`-Enum:** generieren (≈ 2100 Einträge) oder weglassen?

## 12. Abnahmekriterien

- Alle 12 Python-Beispiele aus `opensmi-client/examples/` existieren als C#-Beispiele und laufen
  gegen einen lokal gestarteten `opensmi-server` mit den Dummy-Usern `visitor`, `operator` und
  `orchestrator`.
- Die Integrationstests decken ab: Connect/Retry/Auth-Fehler, Browse mit und ohne Regeln,
  Direct-Node-ID, Skill-Zyklus (Reset → Start → Completed → Reset), Halt während des Waits,
  Suspend, Methodenaufruf, Lock Init/Exit/Break/Renew, AutoLock-Relock nach `BreakLock` durch
  einen höher priorisierten User, Notifications, History, Reconnect mit erhaltenen
  Objektidentitäten.
- 0 Compiler-Warnungen mit `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>` und
  `<Nullable>enable</Nullable>`.
- Kein öffentliches API ohne `CancellationToken`, keine sync-over-async-Aufrufe, keine leeren
  `catch`-Blöcke (Analyzer-Regel).
