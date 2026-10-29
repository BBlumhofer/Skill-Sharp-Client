# 18 History, Security, Reconnect

Type: task
Status: resolved
Blocked by: 10, 13

## Question

`ReadHistoryAsync`, `SecurityOptions` mit persistentem Zertifikat und Trust-Store sowie Reconnect
über den `SessionReconnectHandler` mit erhaltenen Objektidentitäten und Heartbeat-Rebuild. Das
Reconnect-Verhalten des SDK ist noch unklar; dieses Ticket wird nach 07 präzisiert (Fog).

## Stand 2026-10-29

- **Erledigt:** `ReadHistoryAsync` (mit Continuation Points). Auf dem Dummy-Server ist nur Monitoring
  historisiert, ParameterSet nicht; das Python-Orakel sieht dasselbe.
- **Offen:** `SecurityOptions` (persistentes Zertifikat, Trust-Store) und Reconnect über den
  `SessionReconnectHandler` mit Heartbeat-Rebuild. Bisher reagiert der Client nicht auf
  KeepAlive-Fehler; eine abgebrochene Session bleibt tot.

## Answer

History ist erledigt. Der Rest wird aufgeteilt in [21 Security](21-security.md) und
[23 Reconnect](23-reconnect.md); die SDK-Frage dazu klärt [22 Reconnect im SDK](22-reconnect-im-sdk.md).
