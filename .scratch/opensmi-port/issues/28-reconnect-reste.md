# 28 Reconnect-Reste: Watchdog-Option, AutoReconnect=false, Item-Retry

Type: task
Status: resolved
Blocked by: 23, 29

## Question

Offene Punkte aus [23 Reconnect](23-reconnect.md):
1. Das Prüfintervall von 2 s ist fest im Code; es soll über `WatchdogInterval` (SPEC §4.1, Default 1 s) einstellbar sein.
2. Mit `AutoReconnect = false` wird ein toter Server nicht erkannt und der Status bleibt `Connected`;
   erwartet sind `Disconnected` und `StatusChanged`.
3. Fehlgeschlagene einzelne Monitored Items werden nur geloggt und nicht mit Backoff erneut versucht (SPEC §7.1).
4. Kurzer Netzabbruch mit Reactivate derselben Session ist ungetestet (nur Server-Neustart).

## Answer

**1 Watchdog:** `RemoteServerOptions.WatchdogInterval` (Default 1 s) setzt `Session.KeepAliveInterval`, auch bei
`AutoReconnect = false`. Test: `RemoteServerTests.Watchdog_interval_sets_the_keep_alive_period_default_one_second`.

**2 AutoReconnect = false:** Der KeepAlive-Handler hängt jetzt immer an der Session. Ohne Reconnect-Handler ruft ein
Bad-KeepAlive im Status `Connected` `DisconnectAsync` auf (über `Task.Run`, nicht auf dem SDK-Thread). Damit kommen
`Disconnecting` → `Disconnected` samt `StatusChanged`, Subscription und Session werden wie beim normalen Disconnect
abgebaut. Test: `ReconnectTests.Without_auto_reconnect_a_dead_server_ends_disconnected` (Port 4922).

**3 Item-Retry: übersprungen.** Ich habe keine ehrliche Provokation gefunden. Der Dummy-Server legt zur Laufzeit keine
Knoten an, Benutzer dürfen kein AddNodes (nur `UserRole.Admin`), und das einzige realistische Szenario (Neustart, der
Client verbindet sich, bevor der Adressraum fertig ist) lässt sich nicht deterministisch treffen. Ein Test mit einer
nicht existierenden NodeId würde nur zeigen, dass ein Retry versucht wird, nicht dass er etwas heilt. Fehlgeschlagene
Items werden weiter nur geloggt; der Heartbeat-Rebuild legt sie bei Stale mit an.

**4 Reactivate:** Neuer Test-Helfer `TcpProxy` (Tests) zwischen Client und `TestServer`; `Cut()` schließt alle
Verbindungen und nimmt keine neuen an, `Restore()` öffnet wieder. Test
`ReconnectTests.Short_network_drop_reactivates_the_same_session` (Server 4923, Proxy 4924): `Reconnecting` →
`Connected`, dieselbe `Session`-Instanz, dieselbe Maschine, `ValueChanged` feuert wieder. Produktivcode brauchte
dafür keine Änderung, der Pfad in `OnReconnected` war schon richtig.
