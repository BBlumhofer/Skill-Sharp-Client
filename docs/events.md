# Events

## Change events

Objects report changes as .NET events with async handlers.

| Event | On |
|---|---|
| `RemoteServer.StatusChanged`, `TimeUpdated` | connection status, server time |
| `BaseRemoteComponent.StatusChanged` | machinery item state, as `Sample<MachineryItemState?>` |
| `BaseRemoteComponent.NotificationReceived` | a notification of the machine or component |
| `RemoteSkill.StateChanged` | skill state, as `Sample<SkillState>` |
| `RemoteVariable.ValueChanged` | value, as `Sample<object?>` |
| `RemoteLock.LockedChanged`, `LockingUserChanged`, `RenewDue` | lock state |
| `RemoteUser.UserPresenceChanged` | user logged in or out |

```csharp
skill.StateChanged += async (sender, e) =>
    await Console.Out.WriteLineAsync($"{((RemoteSkill)sender).Name}: {e.State} ({e.Sample.Quality})");
```

Value and state events fire when the value or its status changes, not for a new timestamp alone. Handlers run one
after another, in the order they were added, on the client's notification loop. An exception in one handler is
logged and the next one still runs.

Keep handlers short. A handler must not wait for a later notification of the same client, for example with
`StartAsync(StartWait.Completion)`: that notification is only delivered after the handler returns, so the wait runs
into its timeout. Use the streams below or the `Wait…` methods of skills instead.

## Streams

`RemoteSkill.WatchStateAsync`, `RemoteVariable.WatchValueAsync` and `RemoteServer.WatchStatusAsync` return an
`IAsyncEnumerable` that yields the current value first and then every change.

```csharp
await foreach (var state in skill.WatchStateAsync(cancellationToken))
    if (state == SkillState.Completed) break;
```

Each enumeration has its own small buffer. A slow reader loses the oldest values but always gets the newest one.

## Notifications

`NotificationReceived` delivers the OPC UA events of a machine's or component's `Notification` node as
`NotificationEvent(Message, Severity, ErrorCode)`. The error code comes from the MachineSet `AlertType`, e.g. `E123`
when a call lacks the lock. For older servers that append the code as `"<message>|||<code>"`, the client splits it
off. Events reach the object whose `Notification` node was subscribed, even if their `SourceNode` is a child object
such as a method.
