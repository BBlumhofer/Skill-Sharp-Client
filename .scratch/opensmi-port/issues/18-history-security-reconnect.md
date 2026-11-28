# 18 History, Security, Reconnect

Type: task
Status: resolved
Blocked by: 10, 13

## Question

`ReadHistoryAsync`, `SecurityOptions` with a persistent certificate and trust store, and reconnect
through the `SessionReconnectHandler` with preserved object identities and a heartbeat rebuild. The
reconnect behavior of the SDK is still unclear; this ticket will be refined after 07 (fog).

## State on 2026-10-29

- Done: `ReadHistoryAsync` (with continuation points). On the dummy server only Monitoring
  is historized, ParameterSet is not; the Python oracle sees the same.
- Open: `SecurityOptions` (persistent certificate, trust store) and reconnect through the
  `SessionReconnectHandler` with a heartbeat rebuild. So far the client does not react to
  KeepAlive errors; a broken session stays dead.

## Answer

History is done. The rest is split into [21 Security](21-security.md) and
[23 Reconnect](23-reconnect.md); the related SDK question is answered by [22 Reconnect in the SDK](22-reconnect-im-sdk.md).
