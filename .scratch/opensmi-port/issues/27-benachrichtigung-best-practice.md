# 27 Best Practice für Benachrichtigungen in .NET-Bibliotheken

Type: research
Status: resolved
Blocked by: –

## Question

Für Ticket 25: Wie melden etablierte .NET-Bibliotheken mit vergleichbaren Szenarien
(langlebige Verbindung, Server pusht Wertänderungen, Zustandsautomaten, mehrere Abonnenten)
Ereignisse an den Nutzer? Und was empfehlen Microsofts Framework Design Guidelines?

Verglichen werden **.NET-Events**, **`IObservable<T>`/System.Reactive** und
**`IAsyncEnumerable<T>`/Channels**: Threading, Reihenfolge, Ausnahmeverhalten, async-Handler,
Abmelden, Backpressure, Abhängigkeiten, Testbarkeit und Ergonomie für Nutzer (UI, Worker-Service,
Skripte).

Untersucht werden mindestens: OPC Foundation UA-.NETStandard (MonitoredItem.Notification,
Session.KeepAlive), ein kommerzieller OPC-UA-Client mit „moderner“ API (z. B. Workstation.UaClient /
Traeger, Opc.UaFx), MQTTnet (v4/v5, `ApplicationMessageReceivedAsync`), Microsoft.Azure.Devices.Client,
SignalR-Client (`On<T>`/Streaming), gRPC-Streaming, NATS.Client.Core (`SubscribeAsync` als
`IAsyncEnumerable`), StackExchange.Redis Pub/Sub, RabbitMQ.Client 7 (`AsyncEventHandler`), Orleans
Streams, Akka.NET. Dazu jeweils: welches Muster, warum (Release Notes oder Design-Dokus), und ob es
gewechselt wurde (z. B. MQTTnet von Handlern zu async Events, RabbitMQ zu `AsyncEventingBasicConsumer`).

## Answer

Vergleichbare Netzwerk-Clients sind von Sync-Handlern auf Task-zurückgebende Events umgestiegen (MQTTnet 4, RabbitMQ 7, Azure IoT v2). SignalR, NATS und Azure.Core machen es ebenso. Die Handler laufen dort **sequenziell** aus einer Single-Reader-Queue, Ausnahmen werden pro Handler isoliert.
Zum Warten und Konsumieren bieten sie zusätzlich `IAsyncEnumerable` über begrenzte Channels an (NATS, gRPC, SignalR-Streaming, Redis, OPC Foundation 2.0 `IStreamingSubscription` mit `TakeUntilAsync` für „state-machine waits“). Rx als Haupt-API hat nur Workstation.UaClient.
Die FDG (2008) empfehlen `EventHandler<T>` mit void-Rückgabe. Neuere Microsoft-Clients (SignalR, Azure.Core) weichen davon ab, ausdrücklich um `async void` zu vermeiden.
Empfehlung (abweichend von Ticket 25): Hybrid aus `event AsyncEventHandler<XxxEventArgs>` (sequenziell, Fehler pro Handler geloggt), `Watch…Async` als `IAsyncEnumerable` (aktueller Wert zuerst, DropOldest-Puffer) und den bestehenden `WaitFor…Async`. Kein Rx.
Was sich ändert: `AsyncSignal` entfällt, die Handler laufen sequenziell statt per WhenAll, die Signaturen werden zu `(sender, EventArgs)`. Handler, die auf künftige Notifications desselben Clients warten, blockieren die Dispatch-Schleife (gilt schon heute) und müssen das künftig über Watch/WaitFor lösen.
Details: [../research/27-benachrichtigung-best-practice.md](../research/27-benachrichtigung-best-practice.md)
