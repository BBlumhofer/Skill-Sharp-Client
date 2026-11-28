# 16 Notifications

Type: task
Status: resolved
Blocked by: 09

## Question

Event subscription on `Notification` with a fallback to `Messages`. Parsing of
`"<msg>|||<code>"` works as in Python; the ErrorCode from the event takes precedence. Plus
`NotificationReceived`. Oracle: `remote_component.py:285-392`.

## Acceptance

- Unit: `"Error|||E42"` → (`Error`, `E42`); an ErrorCode from the event takes precedence
- Integration: a skill run that produces a notification raises `NotificationReceived`

## Answer

Parsing in `NotificationEvent.Parse`, events through the SubscriptionManager. The fallback to `Messages` (old v4 servers) is missing (`shortcut:`); it belongs in the SmartFactory package. Tests: unit test plus `Skill_run_delivers_notifications`.
