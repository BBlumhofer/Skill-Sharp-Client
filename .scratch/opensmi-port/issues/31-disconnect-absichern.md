# 31 DisconnectAsync gegen gleichzeitige Aufrufe absichern

Type: task
Status: resolved
Blocked by: 30

## Question

`DisconnectAsync` ist nicht gegen gleichzeitige Aufrufe abgesichert, etwa wenn ein toter Server ohne
AutoReconnect den Abbau startet und der Nutzer zur selben Zeit `DisconnectAsync` ruft (siehe 28).
Zwei Aufrufe sollen genau einen Abbau auslösen; beide kehren erst zurück, wenn `Disconnected` erreicht
ist. Der Item-Retry (SPEC §7.1) bleibt bewusst offen, bis ein echter Server das Problem zeigt.

## Akzeptanz

- zwei gleichzeitige `DisconnectAsync` → genau eine Folge `Disconnecting` → `Disconnected`, keine Ausnahme

## Answer

`DisconnectAsync` teilt jetzt einen Abbau-Task: Der erste Aufruf startet `Disconnecting` → Abbau →
`Disconnected`, alle weiteren (auch der Keep-alive-Abbau ohne AutoReconnect und `DisposeAsync`) warten
auf denselben Task. Ruft ein `StatusChanged`-Handler für `Disconnecting` selbst `DisconnectAsync`, kehrt
der Aufruf sofort zurück, statt auf den Abbau zu warten, der auf ihn wartet (kein neuer Deadlock). Nach
dem Abbau verbindet dieselbe Instanz wieder mit `ConnectAsync`, wie bisher. Test:
`RemoteServerTests.Concurrent_disconnects_tear_down_once_and_the_instance_reconnects`.
