# 28 Reconnect-Reste: Watchdog-Option, AutoReconnect=false, Item-Retry

Type: task
Status: open
Blocked by: 23, 29

## Question

Offene Punkte aus [23 Reconnect](23-reconnect.md):
1. Das Prüfintervall von 2 s ist fest im Code; es soll über `WatchdogInterval` (SPEC §4.1, Default 1 s) einstellbar sein.
2. Mit `AutoReconnect = false` wird ein toter Server nicht erkannt und der Status bleibt `Connected`;
   erwartet sind `Disconnected` und `StatusChanged`.
3. Fehlgeschlagene einzelne Monitored Items werden nur geloggt und nicht mit Backoff erneut versucht (SPEC §7.1).
4. Kurzer Netzabbruch mit Reactivate derselben Session ist ungetestet (nur Server-Neustart).
