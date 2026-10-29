# 04 Git-Repo reparieren, Zugangsdaten entschärfen

Type: task
Status: open (HITL, Rest: Commit + Passwörter)
Blocked by: –

## Question

Siehe UMBAUPLAN P0. Im lokalen `.git` fehlt `HEAD`, `main` steht auf `9a0a6c2`, origin auf `e57b3c0`,
und es gibt einen ungeprüften Stash `c19961d`. In der öffentlichen Historie stehen Zugangsdaten
(`visitor/visitor`, `orchestrator/orchestrator`, interne IP).

## Checkliste (Mensch)

- [ ] `echo ref: refs/heads/main> .git\HEAD`, `git fetch origin`, Stash prüfen, `git reset --soft origin/main`
- [ ] Branch `opensmi-port` anlegen und die neuen Dateien committen
- [ ] klären, ob die SmartFactory-Passwörter echt sind, und sie gegebenenfalls rotieren
- [ ] `bin/`, `obj/`, `.vs/` und `TestResults/` aus dem Index entfernen (`.gitignore` ist bereits angelegt)

## Answer

Stand 2026-10-29 (vom Agenten auf Wunsch des Nutzers erledigt):
- Neben dem fehlenden `HEAD` war auch der Objektspeicher defekt: Commit `b8a1eac` fehlte, der Stash
  `c19961d` verwies auf nicht vorhandene Objekte und war nicht mehr lesbar.
- Repariert durch frische Git-Metadaten aus `origin` (GitHub). Das alte Verzeichnis liegt als
  `.git.broken/` (in `.gitignore`), am Arbeitsbaum hat sich nichts geändert. `main` = `e57b3c0`,
  neuer Branch `opensmi-port`, noch nichts committet (Agenten arbeiten noch).
- Offen: der erste Commit auf `opensmi-port`, sobald 19/21/23 fertig sind; Prüfung und Rotation der
  Passwörter `visitor`/`orchestrator` in der SmartFactory (Mensch).
