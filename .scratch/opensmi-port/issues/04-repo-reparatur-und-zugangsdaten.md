# 04 Repair the Git repo, defuse credentials

Type: task
Status: resolved
Blocked by: none

## Question

See UMBAUPLAN P0. The local `.git` is missing `HEAD`, `main` points to `9a0a6c2`, origin to `e57b3c0`,
and there is an unchecked stash `c19961d`. The public history contains credentials
(`visitor/visitor`, `orchestrator/orchestrator`, an internal IP).

## Checklist (human)

- [ ] `echo ref: refs/heads/main> .git\HEAD`, `git fetch origin`, check the stash, `git reset --soft origin/main`
- [ ] create branch `opensmi-port` and commit the new files
- [ ] find out whether the SmartFactory passwords are real and rotate them if so
- [ ] remove `bin/`, `obj/`, `.vs/` and `TestResults/` from the index (`.gitignore` already exists)

## Answer

State on 2026-10-29 (done by the agent at the user's request):
- Besides the missing `HEAD`, the object store was also broken: commit `b8a1eac` was missing, the stash
  `c19961d` referred to objects that did not exist and could no longer be read.
- Repaired with fresh Git metadata from `origin` (GitHub). The old directory is kept as
  `.git.broken/` (in `.gitignore`); nothing changed in the working tree. `main` = `e57b3c0`,
  new branch `opensmi-port`, nothing committed yet (agents are still working).
- Open: the first commit on `opensmi-port` once 19/21/23 are done; checking and rotating the
  passwords `visitor`/`orchestrator` in the SmartFactory (human).

Addendum 2026-10-29: according to the user, `visitor`/`orchestrator` are only test accounts of the
dummy server; nothing to rotate, the history stays. The branch `opensmi-port` is committed.
