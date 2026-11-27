# Spezifikation: OpenSMI-Client für C# (`OpenSmi.Client`)

Hinweis: Dieses Dokument wurde mit KI-Unterstützung (Claude, Anthropic) erstellt; siehe den KI-Hinweis im README.

Stand: 2026-11-27 (Version 2.0.0). Ursprünglich am 2026-10-29 vor der Umsetzung geschrieben und seitdem an
den gebauten Client (`src/OpenSmi.Client`) angepasst. Wo sich der Plan geändert hat, nennt der Text das Ticket in
`.scratch/opensmi-port/issues/`. Referenz-Snapshots: `SmartFactory-KL/opensmi-client` @ `9026539`
(opensmi-core 0.7.1), `opensmi-server` 0.3.1, alter `Skill-Sharp-Client` @ `e57b3c0` (Tag `skillsharp-legacy-final`).
Die erste Portfassung folgte `opensmi-client` @ `e8740c2`; was sich mit 2.0.0 geändert hat, steht in
[CHANGELOG.md](../../CHANGELOG.md) und Ticket 34. Die Inventare in `analyse/` beschreiben den Stand `e8740c2`.
Die Detail-Inventare mit `file:line`-Belegen liegen in [`analyse/`](analyse/). Diese Spec verweist
auf sie mit [OI §x] (OpenSMI-Inventar) und [SI §x] (Skill-Sharp-Inventar).

## 1. Ziel und Abgrenzung

Ziel ist eine C#-Bibliothek, die sich gegenüber einem SMI-konformen OPC-UA-Server (z. B.
`opensmi-server`) so verhält wie der Python-Client `opensmi-client`. Sie bildet dasselbe
Objektmodell, dieselben Pfade, dieselbe Browse-Logik und dieselbe Skill- und Lock-Semantik ab, mit
einer API, die sich in C# natürlich anfühlt.

Nicht-Ziele:
- Kein 1:1-Transkript von Python. Wo Python-Idiome (Context-Manager, `asyncio.timeout`, Signals per
  `set`) keine natürliche Entsprechung haben, gilt die C#-Konvention (§3).
- Keine Server-Implementierung, kein Port von `opensmi-core` jenseits dessen, was der Client braucht.
- SmartFactory-spezifische Funktionen (Ports, Storages, RFID-Nachbarn, Startup/Recovery) gehören
  nicht in den Kern. Sie kommen in ein optionales Erweiterungspaket (§9).
- Keine API-Kompatibilität zum alten `SkillSharp.Client`. IMAPRO hängt nicht mehr davon ab
  [SI §7]. Version 2.0.0 erscheint trotzdem unter dessen Paketnamen und ersetzt ihn (§2).

Bewusste Abweichungen vom Python-Client (dort Bugs oder Unschärfen, [OI Zusammenfassung])
stehen in §10.

## 2. Pakete und Projektstruktur

| Projekt | Inhalt | Abhängigkeiten |
|---|---|---|
| `src/OpenSmi.Client` | Kern: Server, Machine, Component, Resource, Skill, Check, Method, Variable, Lock, AutoLock, User, Pfade, Browse-Regeln, Samples, Subscriptions | `OPCFoundation.NetStandard.Opc.Ua.Client` (1.5.377.x), `Microsoft.Extensions.Logging.Abstractions` |
| `src/OpenSmi.Client.SmartFactory` (optional, noch nicht begonnen) | Ports, Storages, Nachbarn, Startup/Recovery auf Basis des Kerns | `OpenSmi.Client` |
| `tests/OpenSmi.Client.Tests` | Unit-Tests ohne Server und Integrationstests gegen einen lokal gestarteten `opensmi-server` (Python), in einem Projekt (Ticket 05) | xUnit, Python 3.12 im CI |
| `examples/OpenSmi.Client.Examples` | C#-Pendants zu `opensmi-client/examples/*.py` in einem Konsolenprojekt | `OpenSmi.Client` |

- Target Frameworks: `net8.0` und `net10.0`, die Beispiele nur `net10.0` (Ticket 24).
- Root-Namespace = Assembly-Name: `OpenSmi.Client`. Damit endet der Wildwuchs aus `UAClient.*` und
  `SkillSharp.Client` [SI §1]. Die PackageId bleibt `SkillSharp.Client`; 2.0.0 folgt auf die 1.x-Pakete
  des alten Clients (Ticket 34).
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
| Nebenläufigkeit | asyncio single-threaded | Notifications kommen auf SDK-Threads. Cache-Felder sind `volatile` oder lock-geschützt; Events werden nicht auf dem SDK-Publish-Thread awaitet, sondern über einen `Channel<T>` je Server seriell abgearbeitet (Reihenfolge je Node bleibt erhalten) |
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
    public string ApplicationUri { get; init; } = $"urn:uuid:{Guid.NewGuid()}";
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan WatchdogInterval { get; init; } = TimeSpan.FromSeconds(1);   // KeepAlive-Periode
    public bool AutoReconnect { get; init; } = true;
    public TimeSpan ReconnectMaxDelay { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan HeartbeatStaleAfter { get; init; } = TimeSpan.FromSeconds(10); // §7.1
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
    public IReadOnlyDictionary<string, RemoteMachine> Machines { get; }   // nach Segment
    public IReadOnlyDictionary<string, RemoteUser> Users { get; }         // User des Servers, nach Segment
    public ObjectPath Path { get; }                                       // immer "/"
    public string Username { get; }
    public IReadOnlyList<Exception> InitExceptions { get; }     // §8.2
    public ISession? Session { get; }                           // Escape-Hatch, entspricht ua_client

    public event AsyncEventHandler<ServerStatusChangedEventArgs>? StatusChanged;
    public event AsyncEventHandler<ServerTimeUpdatedEventArgs>? TimeUpdated;
    public IAsyncEnumerable<RemoteServerStatus> WatchStatusAsync(CancellationToken ct = default);

    public RemoteUaObject? GetUaObject(NodeId nodeId);
    public RemoteUaObject Resolve(ObjectPath path);                       // auch string; KeyNotFoundException
    public RemoteUaObject? ResolveOptional(ObjectPath path);
    public IReadOnlyList<RemoteUaObject> Find(string pattern);            // Glob, Baumreihenfolge, Aliase nicht doppelt
    public ushort GetNamespaceIndex(string namespaceUri);       // KeyNotFoundException
}
```

- Ablauf von `ConnectAsync` wie [OI §2.2]. Retry mit exponentiellem Backoff
  (`ConnectRetryOptions(MaxRetries = 3, MinDelaySeconds = 0.5, MaxDelaySeconds = 30)`, `MaxRetries = null`
  heißt unendlich). `BadIdentityTokenRejected` wird erneut versucht.
  `BadUserAccessDenied`/`BadIdentityTokenInvalid` → `OpenSmiAuthenticationException`
  (entspricht `ConnectionRefusedError`). Ein nicht vertrauenswürdiges Server-Zertifikat führt ohne
  Retry zu `OpenSmiConnectionException` (Ticket 21).
- Es gibt nur einen Reconnect-Mechanismus: den `SessionReconnectHandler` des SDK. Der zweite
  Polling-Loop aus SSC entfällt [SI §2.2]. Mit `AutoReconnect = false` geht ein toter Server auf
  `Disconnected` (Ticket 28).
- Nach einem Reconnect bleiben die Objekt-Identitäten erhalten. Es wird nicht neu gebrowst: Das SDK
  legt die Subscription neu an, und der SubscriptionManager hängt sich um (Ticket 23). SSC erzeugte
  dabei neue `RemoteModule`-Instanzen.
- `DisposeAsync` ruft `DisconnectAsync`. Gleichzeitige Aufrufe teilen sich einen Abbau (Ticket 31).

### 4.2 `RemoteUaObject` (abstrakte Basis) [OI §2.0]

`Name` (BrowseName, auch leer oder mit `/`), `Segment` (eindeutig unter den Geschwistern),
`Path` (`ObjectPath`, z. B. `/M1/Components/Gripper/SkillSet/Pick`), `Aliases`, `NodeId`, `UaType`
(BrowseName der TypeDefinition, Default `"UNKNOWN"`), `Parent`, `Server`, `BrowseRules`,
`InitExceptions`, `IsInitialized`, `InitAsync(ct)`, `ReadUaTypeAsync(ct)`, `Resolve(relative)`,
`ResolveOptional(relative)`, `Find(pattern)`. Beim Setzen der `NodeId` registriert sich das Objekt im
Server-Register nach NodeId und Pfad (Weak-Referenzen, entspricht `WeakValueDictionary`); Eltern registrieren
sich vor ihren Kindern, danach ist der Pfad fest.

Der Pfad ist wie in opensmi-core 0.7: Elternpfad, OpenSMI-Ordner und Segment. Ordner sind `Components`
(Komponenten), `Resources`, `SkillSet` (Skills), `MethodSet` (Methoden) und `Users` (User, Eltern ist der
Server). Variablengruppen (`Monitoring`, `ParameterSet`, …) und der `Lock` sind selbst Objekte mit ihrem Namen
als Segment. Checks liegen unter ihrem Skill (`/M1/SkillSet/Pick/FeasibilityCheck`). Das Segment entsteht aus dem
BrowseName: `/` wird `_`, ein leerer Name wird `_`, doppelte Namen bekommen in NodeId-Reihenfolge (Namespace-URI,
Identifier-Typ, Identifier) `_2`, `_3`, … (`PathSegments.Unique`, Port von `paths.unique_segments`).

### 4.3 `BaseRemoteComponent`, `RemoteComponent`, `RemoteMachine`, `RemoteResource` [OI §2.3–2.6]

```csharp
public abstract class BaseRemoteComponent : RemoteUaObject
{
    public IReadOnlyDictionary<string, RemoteSkill> SkillSet { get; }
    public IReadOnlyDictionary<string, RemoteSkillCheck> FeasibilityCheckSet { get; }   // nach Segment des Skills
    public IReadOnlyDictionary<string, RemoteSkillCheck> PreconditionCheckSet { get; }
    public IReadOnlyDictionary<string, RemoteMethod> MethodSet { get; }
    public IReadOnlyDictionary<string, RemoteComponent> Components { get; }
    public IReadOnlyDictionary<string, RemoteResource> Resources { get; }
    public Identification Identification { get; }
    public Monitoring Monitoring { get; }
    public Attributes Attributes { get; }
    public ParameterSet ParameterSet { get; }
    public MachineryItemState? CurrentState { get; }                 // null ohne CurrentState-Knoten
    public Sample<MachineryItemState?>? CurrentStateSample { get; }
    public Task<MachineryItemState?> ReadCurrentStateAsync(CancellationToken ct = default);
    public event AsyncEventHandler<ComponentStatusChangedEventArgs>? StatusChanged;
    public event AsyncEventHandler<NotificationReceivedEventArgs>? NotificationReceived;
}
public sealed class RemoteComponent : BaseRemoteComponent { public RemoteLock? Lock { get; } }
public sealed class RemoteMachine   : BaseRemoteComponent { public RemoteLock Lock { get; } }
public sealed class RemoteResource  : BaseRemoteComponent { }
```

- Alle Collections sind nach Segment geschlüsselt.
- `RemoteMachine.Lock` wirft `InvalidOperationException` mit klarer Meldung, wenn das Feature
  `Lock` nicht gebrowst wurde. Python wirft hier einen `AssertionError`.
- `CurrentState` wird wie in Python geparst (Groß-/Kleinschreibung, Leerzeichen und `_` egal). Leere und
  unbekannte Werte im Subscription-Pfad behalten den letzten gültigen Zustand; ein Bad-Status ergibt `null`.
  `StatusChanged` kommt nur, wenn sich Wert oder Status ändern. `ReadCurrentStateAsync` wirft bei unbekanntem
  Wert `OpenSmiUnknownStateException`. Der Zustand wird beim Browsen einmal gelesen (Ticket 34).
- User gehören nicht mehr zur Maschine, sondern zum Server (§4.7).

### 4.4 `RemoteCallable`, `RemoteMethod`, `RemoteSkill` [OI §2.7–2.9, §5]

```csharp
public abstract class RemoteCallable : RemoteUaObject
{
    public ParameterSet ParameterSet { get; }
    public Monitoring Monitoring { get; }
    public FinalResultData FinalResultData { get; }
    public int? MinAccessLevel { get; }
    public IReadOnlyList<NodeId> DependsOn { get; }      // Requires vorwärts aus Requirements
    public IReadOnlyList<NodeId> DependencyOf { get; }   // Requires invers
}

public sealed class RemoteMethod : RemoteCallable
{
    // Ruft "Call" auf dem Method-Objekt OHNE Eingangsargumente, liefert danach FinalResultData.ReadAllAsync().
    public Task<IReadOnlyDictionary<string, object?>> CallAsync(CancellationToken ct = default);
}

public class RemoteSkill : RemoteCallable
{
    public SkillState CurrentState { get; }          // gecacht, ohne Wert Halted
    public Sample<SkillState> CurrentStateSample { get; }
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
}

public sealed class RemoteSkillCheck : RemoteSkill
{
    public RemoteSkill Skill { get; }                // Pfad = Skill.Path / "FeasibilityCheck" bzw. "PreconditionCheck"
}

public enum StartWait { None, RunningOrCompleted, Completion }
```

- Die `NodeId` des Skills ist der `SkillExecution`-Knoten, `Name` ist der BrowseName des Skill-Objekts
  darüber. Ein Skill-Objekt ohne `SkillExecution` wird mit Warnung übersprungen. Checks sind
  `RemoteSkillCheck` mit dem Check-Knoten als `NodeId` und dem BrowseName des Checks als Name und Segment.
  `IsFinite` liest die TypeDefinition des Skill-Objekts [OI §2.9].
- `StateChanged` trägt ein `Sample<SkillState>` und kommt bei Wert- oder Statusänderung; der Halt-Zähler
  und die Waits reagieren nur auf Wertänderungen. Der Zustand wird beim Browsen einmal gelesen.
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
- Unbekannter Zustandstext: `ReadCurrentStateAsync` wirft `OpenSmiUnknownStateException` (eine
  `OpenSmiProtocolException`) statt `KeyError`. Im Subscription-Pfad wird er geloggt und ignoriert. Ein leerer Text bedeutet
  `Halted` (wie Python).
- `SuspendAsync` ohne Suspend-Knoten wirft `SkillNotSuspendableException`.

### 4.5 `RemoteVariable`, Variable-Container [OI §2.10–2.11]

```csharp
public sealed class RemoteVariable : RemoteUaObject
{
    public Sample<object?> Sample { get; }        // Wert, Variant, Status, Quality, Source-/ServerTimestamp
    public object? Value { get; }                 // = Sample.Value, verarbeitet (Enum-Mapping, NodeId → DisplayName-Key …)
    public Variant RawValue { get; }
    public DateTime? Timestamp { get; }           // SourceTimestamp, sonst ServerTimestamp
    public bool IsWritable { get; }               // aus UserAccessLevel (CurrentWrite), NICHT aus dem Ordnernamen
    public string UaDataType { get; }
    public int UaValueRank { get; }               // aus dem Read, sonst -1 (Skalar)
    public IReadOnlyDictionary<string, string> ValidValues { get; }
    public EUInformation? Unit { get; }
    public Opc.Ua.Range? Range { get; }
    public event AsyncEventHandler<VariableValueChangedEventArgs>? ValueChanged;
    public IAsyncEnumerable<Sample<object?>> WatchValueAsync(CancellationToken ct = default);

    public Task<object?> ReadValueAsync(bool updateCache = false, CancellationToken ct = default);
    public Task<Variant> ReadRawValueAsync(CancellationToken ct = default);
    public Task<Sample<object?>> ReadSampleAsync(bool updateCache = false, CancellationToken ct = default);
    public Task WriteValueAsync(object? value, CancellationToken ct = default);
    public Task<IReadOnlyList<Sample<object?>>> ReadHistoryAsync(DateTime? start = null, DateTime? end = null,
                                                                  uint numValues = 0, bool returnBounds = true,
                                                                  CancellationToken ct = default);
}

public abstract class RemoteVariableContainer : RemoteUaObject, IReadOnlyDictionary<string, RemoteVariable>
{
    public Task<IReadOnlyDictionary<string, object?>> ReadAllAsync(CancellationToken ct = default);   // EIN Read-Request (Batch)
    public Task WriteAllAsync(IReadOnlyDictionary<string, object?> values, CancellationToken ct = default); // EIN Write-Request
}
public sealed class Attributes, Identification, Monitoring, ParameterSet, FinalResultData : RemoteVariableContainer;
```

- Typkonvertierung beim Schreiben: Der Ziel-`BuiltInType` kommt aus dem gecachten
  `RawValue.TypeInfo` bzw. dem DataType. `int` → `Double` usw. wird konvertiert. In SSC fehlte das,
  Folge war `BadTypeMismatch` [SI §2.11]. `string` → `NodeId` und `string` → `LocalizedText(en-US)`
  wie in `ua_node_util.write_value` [OI §2.10].
- Bei abgelehntem Wert gilt die Fallback-Logik über `ValidValues`-Displaynamen wie in Python.
  Danach kommt `OpenSmiInvalidValueException`.
- `ReadAllAsync`/`WriteAllAsync` arbeiten gebatcht: ein Request statt N sequenzieller
  Requests wie in Python. Bei `WriteAllAsync` mit unbekanntem Namen fliegt
  `KeyNotFoundException`, bevor überhaupt geschrieben wird.
- Eine Variable, die in mehreren Gruppen steht, ist ein Objekt. Besitzer ist die Gruppe mit `HasComponent`
  (bzw. einer anderen Referenz als `Organizes`); dort liegt der Pfad. Gruppen mit `Organizes` führen sie als
  Alias, nachdem alles gebrowst ist. Referenzieren mehrere Gruppen sie mit `HasComponent`, gewinnt die erste in
  NodeId-Reihenfolge (Warnung). Nur Referenztypen entscheiden, NodeIds werden nie interpretiert.
- `ValueChanged` trägt das neue `Sample` und kommt nur bei Wert- oder Statusänderung, nicht bei einem neuen
  Zeitstempel allein (OPC-UA-Standard-Trigger). Bis zum ersten Wert hat das Sample `BadWaitingForInitialData`.
- Custom-Enum-Datentypen (Namespace > 0) liest der Client über `DataTypeDefinition`, ohne das
  ComplexTypes-Paket: `ValidValues` bildet die Zahl auf den Membernamen ab, `Value` ist der
  Membername, geschrieben wird per Name oder Zahl. Structs bleiben `ExtensionObject` (Ticket 30).

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

- Die Eigentümerschaft wird ausschließlich über `LockingUser == Username` bestimmt, wie in Python.
  Die SSC-Heuristik (ApplicationName, Hostname, IP, localhost, „unlesbar = gehört uns“) ist
  falsch und fällt weg [SI §2.3].
- Methoden werden exakt per BrowseName aufgelöst (`InitLock`, `BreakLock`, `ExitLock`,
  `RenewLock`), ohne Teilstring-Fallback und ohne Durchprobieren weiterer Methoden. In SSC
  konnte `InitLock` dadurch in `ExitLock` laufen [SI §2.9].
- Der Integer-Rückgabewert des Servers wird ausgewertet und als `LockResult` geliefert. Python
  ignoriert ihn [OI §6].
- `InitLock` wird ohne Argumente aufgerufen, so wie es der opensmi-server erwartet
  ([OI §6]: optionales `context`). Einen `context`-Parameter gibt es nicht.
- `MaxInactiveLockTime` wird aus DI `i=6387` gelesen, Fallback 60 000 ms.
- `AutoLock`: Verhalten wie [OI §6] (bei Verlust neu sperren, bei `RenewDue` erneuern, ein
  Renew-Task gleichzeitig, Renew-Timeout 5 s). `blocking=true` respektiert `ct`.
  `DisposeAsync` ruft `UnlockAsync`.

### 4.7 `RemoteUser` [OI §2.14]

`Name`, `Data` (`AllowMultiple`, `UserLevel`, `MaxAccessLevel`, `IsPresent`),
`UserPresenceChanged`, `LockPriority` (aus `UserLevel` geparst, `null` wenn nicht numerisch). User gehören
dem Server (`RemoteServer.Users`, Pfad `/Users/<name>`, kein Parent). Gelesen werden sie aus `Objects/Users`
(BrowseName `Users` im MachineSet-Namespace). Fehlt der Ordner (opensmi-server < 0.3), kommen sie aus
`MachineryBuildingBlocks/Users` jeder Maschine, ein Objekt je Knoten. Gebrowst wird nur, wenn die Regeln für `/`
`BrowseFeature.Users` liefern.

### 4.8 `NotificationEvent` [OI §2.15, §7]

`record NotificationEvent(string Message, int Severity, string ErrorCode)`. `ErrorCode` kommt aus dem Feld des
MachineSet-`AlertType`; nur ohne dieses Feld wird ein `"<msg>|||<code>"`-Suffix älterer Server abgetrennt.
Events werden nach Monitored Item zugeordnet, nicht nach `SourceNode`: opensmi-server setzt `SourceNode` auf das
Objekt, das die Meldung erzeugt hat (z. B. eine Methode unter der Maschine). Den Fallback
auf den Kindknoten `Messages` (alte v4-Server, wenn `Notification` `BadServiceUnsupported`
liefert) hat der Kern nicht; er gehört ins SmartFactory-Paket (Ticket 16).

## 5. Browsing [OI §3]

- Einstieg: Machinery `Machines` (`nsu=http://opcfoundation.org/UA/Machinery/;i=1001`). Fallback:
  erstes Kind von `Objects` mit `BrowseName.Name == "Machines"`, unabhängig vom Namespace. Die
  SSC-Fallbacks `SFM:ModuleSet` und „DisplayName enthält Module/Machine“ gehören in das
  SmartFactory-Paket, nicht in den Kern.
- Dispatch der Kinder per exaktem `BrowseName.Name` gemäß Tabelle [OI §3]: `Lock`, `SkillSet`,
  `Notification`, `Identification`, `Attributes`, `Monitoring`, `ParameterSet`, `Components`,
  `Resources`, `MachineryBuildingBlocks`, `MethodSet`. Unbekannte Kinder erzeugen nur eine
  Warnung im Log. Teilstring- oder TypeDefinition-Heuristiken wie in SSC gibt es nicht [SI §4].
- Browse erfolgt rekursiv (Komponenten in Komponenten). Die SSC-Discovery ging nur eine Ebene
  tief [SI §2.3].
- Regeln werden auf dem `ObjectPath` aus §4.2 ausgewertet, also mit Ordnersegmenten
  (`/M1/Components/Gripper`). Maschinen werden unter `/<Segment>` geprüft, User unter `/`.
- `BrowseFeature` als `[Flags] enum`, `BrowseRule(Func<ObjectPath, bool> Match, BrowseFeature Features)`,
  Helfer `BrowseRules.When(match, params BrowseFeature[])`, `Match.All/Name/Path/PathPrefix/Glob/Any/AllOf/Not`,
  `BrowseRule.Default`. `Match.Path`, `Match.PathPrefix` und `Match.Glob` prüfen einen String-Pfad sofort
  (`ArgumentException`). Globs: `*`, `?`, `[...]` je Segment wie `fnmatchcase`, `**` über beliebig viele Segmente.
- `BrowseRules = []` überspringt die Discovery. Danach können Objekte direkt aus bekannten NodeIds
  erzeugt werden (Direct-Node-ID-Modus):
  `await RemoteComponent.CreateAsync(server, name, nodeId, parent: null, rules, ct)`. Ohne Parent ist der
  Pfad `/Components/<name>`.
- Die Inits der Kinder eines Objekts laufen nebenläufig und ohne Begrenzung, wie `gather` in
  Python. Fehler einzelner Kinder landen in `InitExceptions`. Verbindungs-, Timeout- und
  Abbruchfehler brechen ab.
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
| Notifications | Kindknoten `Notification`, Events mit `ErrorCode` aus MachineSet `AlertType` (`i=1001`) |
| Users | `Objects/Users/<User>/{AllowMultiple, UserLevel, MaxAccessLevel, IsPresent}` (vor 0.3: `MachineryBuildingBlocks/Users`) |
| Skill | `SkillSet/<Skill>/SkillExecution`, Checks `SkillSet/<Skill>/{FeasibilityCheck, PreconditionCheck}` |
| Skill-Zustandsmaschine | `…/SkillExecution/StateMachine/{CurrentState, Start, Halt, Reset, [Suspend]}` |
| Skill-Container | `…/SkillExecution/{ParameterSet, Monitoring, FinalResultData, MinAccessLevel}`, `Requirements` am Skill-Objekt (vor 0.3 unter `SkillExecution`) |
| Methode | `MethodSet/<Method>/{Call, ParameterSet, Monitoring, FinalResultData, MinAccessLevel, Requirements}` |
| Abhängigkeiten | Referenztyp `Requires` (vorwärts = `DependsOn`, invers = `DependencyOf`) |
| Variablen-Metadaten | Properties `EngineeringUnits`, `EURange`; NodeId-Werte mit `Utilizes`-Referenzen ergeben `ValidValues` |

`Requirements` lag beim Dummy-Server von opensmi-server `main` 29e4d4a an beiden Stellen (Ticket 01); seit
opensmi-server 0.3 liegt es nur noch am Skill-Objekt. Python liest nur `SkillExecution/Requirements` und findet
dort nichts mehr. Der Client liest beide Stellen; die Checks erben die `Requirements` ihres Skills nicht
(Ticket 34, DEVIATIONS).

`SkillState` (IDs 11 bis 20) und `MachineryItemState` (`NotAvailable`, `OutOfService`, `NotExecuting`,
`Executing`) sind C#-Enums nach `opensmi-core/ua_enums.py`. Werte und Zustände trägt `Sample<T>` (Port von
`client/sample.py`) mit `Quality` aus der Schwere des Status. Das `Unit`-Enum (Rec-20-Tabelle,
2133 Einheiten) erzeugt `tools/generate_units.py` aus opensmi-core, mit Namen in PascalCase
(`Unit.Centimetre`, Ticket 26 und 29). `RemoteVariable.Unit` bleibt `EUInformation`; die Abbildung
auf das Enum geht über `ToUnit()`.

## 7. Subscriptions, History, Security

### 7.1 SubscriptionManager [OI §7]
- Eine gemeinsame Subscription pro Server. Items werden je NodeId dedupliziert, ein Item kann
  mehrere Handler tragen. Das Aufteilen in Chunks erledigt das SDK beim Anlegen (Ticket 07).
- Die Notification verwendet den gelieferten Wert. Die SSC-Praxis „bei jeder Änderung neu
  lesen“ ist verboten [SI §2.13].
- Heartbeat über `CurrentTime`: Kommt länger als `HeartbeatStaleAfter` (Default 10 s) keine
  Notification, wird die Subscription gelöscht und mit allen Data- und Event-Items neu
  angelegt (SSC verlor Event-Items beim Rebind). Fehlgeschlagene Einzel-Items werden nur geloggt
  und, wie in Python, nicht erneut versucht. Ein Retry kommt erst, wenn ein echter Server das
  Problem zeigt (Ticket 28).
- Events werden per `SourceNode` an die registrierten Handler geroutet. Der EventFilter wählt
  `Message`, `Severity`, `SourceNode` und `ErrorCode`.
- Abbestellen gibt es nicht, der Python-Client bestellt auch nie ab (Ticket 07).

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
- Mit Security wird ein selbstsigniertes Zertifikat einmal erzeugt und im Store abgelegt
  (CN „OpenSMI C# Client“, SAN = ApplicationUri). Python erzeugt es bei jedem Connect neu.
  Ohne `CertificateStorePath` liegt der Store unter `%LOCALAPPDATA%/OpenSmi.Client/pki`.
- Server-Zertifikate werden standardmäßig gegen den Trust-Store geprüft (Default
  `<CertificateStorePath>/trusted`). `AutoAcceptServerCertificate` muss man explizit einschalten.
  SSC akzeptierte hart kodiert alles [SI §2.1].
- Der Endpoint wird exakt nach Policy und Mode gewählt; gibt es keinen passenden, kommt sofort
  `OpenSmiConnectionException` (Ticket 21).
- Das Session-Objekt wird über `DefaultSessionFactory`/`ISessionFactory` erzeugt, ohne
  Reflection [SI §2.1].
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
├── OpenSmiProtocolException                         – Pflichtknoten fehlt
│   └── OpenSmiUnknownStateException                 – unbekannter Skill- oder Komponentenzustand
├── OpenSmiInvalidValueException                     – Schreibwert ungültig / nicht beschreibbar
├── SkillHaltedException                             – Skill während eines Waits gehaltet
└── SkillNotSuspendableException
```
`TimeoutException` und `OperationCanceledException` bleiben die Standardtypen.

### 8.2 Grundsätze
- Keine leeren `catch {}`. Jede abgefangene Ausnahme wird geloggt und entweder gesammelt
  (`InitExceptions`) oder weitergereicht. SSC enthält Hunderte stiller Catches [SI §2].
- Fehlende Pflichtknoten (Lock: 8 Knoten; Skill: `CurrentState`, `Start`, `Halt`, `Reset`)
  führen zu `OpenSmiProtocolException` mit Pfad, nicht zu `AssertionError`.
- Exceptions aus Signal-Handlern werden geloggt und erreichen den Sender nicht.

## 9. SmartFactory-Erweiterung (`OpenSmi.Client.SmartFactory`, optional)

Noch nicht begonnen; das Paket ist ein eigenes Vorhaben nach dem Client (Ticket 03). Es ist nur
nötig, wenn reale SmartFactory-KL-Module (CA-Module & Co.) angesteuert werden sollen, und baut auf
dem Kern auf statt ihn zu forken:

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
| `user_level` = Lock-Priorität als Text | zusätzlich `LockPriority` | Benennungsfehler [OI §2.14] |
| `read_current_state` → `KeyError` / `UnknownStateError` | `OpenSmiUnknownStateException` | robustere Fehlerklasse |
| Zustände bis zur ersten Notification unbekannt | beim Browsen gelesen | nach `ConnectAsync` verlässlich (Ticket 34) |
| `Requirements` nur unter `SkillExecution` | auch am Skill-Objekt | opensmi-server 0.3 legt sie dort ab (Ticket 34) |
| `read_all` nach Name, `write_all` nach Segment | beide nach Segment | gleiche Schlüssel (Ticket 34) |
| Lock-Rückgabewerte ignoriert | `LockResult` | Information nicht verwerfen [OI §6] |
| Zertifikat pro Connect neu, kein Trust-Store | persistentes Zertifikat, Trust-Store | Sicherheit [OI §7] |
| `read_all`/`write_all` sequenziell | gebatcht | Performance |

Dazu kommen das Benachrichtigungsmodell (§3), die Unit-Namen (§6) und die Custom-Enum-Typen
(§4.5). Jede Abweichung hat einen Test und einen Eintrag in [DEVIATIONS.md](DEVIATIONS.md). Wenn
upstream den Fehler behebt, wird das Verhalten angeglichen.

## 11. Entscheidungen

Entschieden in Ticket 03 (2026-10-29), den Folgetickets 24 bis 27 und Ticket 34 (2026-11-27):

1. Repo: `OpenSmi.Client` bleibt im Repo `Skill-Sharp-Client` und erscheint als Paket `SkillSharp.Client` 2.0.0.
   Umbenennung des Repos, Veröffentlichung auf NuGet und eine Anfrage bei SmartFactory-KL warten, bis klar ist,
   ob SmartFactory-KL den Client aufnimmt (Issue #9).
2. Target Frameworks: `net8.0` und `net10.0`.
3. SmartFactory-Paket: kommt nach dem Client als eigenes Vorhaben.
4. Benachrichtigungen: async Events, `Watch…Async` als `IAsyncEnumerable` und `WaitFor…Async`
   statt `AsyncSignal<T>`; kein Rx.
5. `Unit`-Enum: wird generiert.

## 12. Abnahmekriterien

Stand 2026-11-27 (2.0.0): Die Tests in `tests/OpenSmi.Client.Tests` laufen in der CI unter Linux gegen
opensmi-server `v0.3.1` grün, `net8.0` mit 136 und `net10.0` mit 148 Tests. Eine Analyzer-Regel gegen leere
`catch`-Blöcke gibt es nicht (Issue #4); die wenigen bewusst leeren `catch` tragen eine Begründung.

- Alle 11 Python-Beispiele aus `opensmi-client/examples/` (9026539) existieren als C#-Beispiele und laufen
  gegen einen lokal gestarteten `opensmi-server` mit den Dummy-Usern `visitor`, `operator` und
  `orchestrator`.
- Die Integrationstests decken ab: Connect/Retry/Auth-Fehler, Browse mit und ohne Regeln,
  Direct-Node-ID, Skill-Zyklus (Reset → Start → Completed → Reset), Halt während des Waits,
  Suspend, Methodenaufruf, Lock Init/Exit/Break/Renew, AutoLock-Relock nach `BreakLock` durch
  einen höher priorisierten User, Notifications mit Fehlercode, History, Reconnect mit erhaltenen
  Objektidentitäten, Pfade/Resolve/Find/Aliase, User am Server.
- Gegen jedes Beispiel von opensmi-server (`check`, `dictionary_entries`, `dummy`, `quick_start`, `rsl`) und den
  Testserver der Upstream-Tests findet der Client dieselben Objekte wie der Python-Client.
- 0 Compiler-Warnungen mit `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>` und
  `<Nullable>enable</Nullable>`.
- Kein öffentliches API ohne `CancellationToken`, keine sync-over-async-Aufrufe, leere `catch`-Blöcke nur
  mit Begründung im Kommentar.
