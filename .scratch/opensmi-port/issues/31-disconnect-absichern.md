# 31 DisconnectAsync gegen gleichzeitige Aufrufe absichern

Type: task
Status: claimed
Blocked by: 30

## Question

`DisconnectAsync` ist nicht gegen gleichzeitige Aufrufe abgesichert, etwa wenn ein toter Server ohne
AutoReconnect den Abbau startet und der Nutzer zur selben Zeit `DisconnectAsync` ruft (siehe 28).
Zwei Aufrufe sollen genau einen Abbau auslösen; beide kehren erst zurück, wenn `Disconnected` erreicht
ist. Der Item-Retry (SPEC §7.1) bleibt bewusst offen, bis ein echter Server das Problem zeigt.

## Akzeptanz

- zwei gleichzeitige `DisconnectAsync` → genau eine Folge `Disconnecting` → `Disconnected`, keine Ausnahme
