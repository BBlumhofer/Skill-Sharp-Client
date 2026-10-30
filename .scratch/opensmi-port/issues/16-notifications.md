# 16 Notifications

Type: task
Status: resolved
Blocked by: 09

## Question

Event-Subscription auf `Notification` mit Fallback auf `Messages`. Das Parsing von
`"<msg>|||<code>"` funktioniert wie in Python, der ErrorCode aus dem Event hat Vorrang. Dazu
`NotificationReceived`. Orakel: `remote_component.py:285-392`.

## Akzeptanz

- Unit: `"Fehler|||E42"` → (`Fehler`, `E42`); ein ErrorCode aus dem Event hat Vorrang
- Integration: Ein Skill-Lauf, der eine Notification erzeugt, löst `NotificationReceived` aus

## Answer

Parsing in `NotificationEvent.Parse`, Events über den SubscriptionManager. Der Fallback auf `Messages` (alte v4-Server) fehlt (`shortcut:`), er gehört ins SmartFactory-Paket. Tests: Unit-Test plus `Skill_run_delivers_notifications`.
