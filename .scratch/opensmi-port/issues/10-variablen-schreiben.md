# 10 Variablen schreiben, ReadAll/WriteAll, ValueChanged

Type: task
Status: resolved
Blocked by: 09, 13

## Question

`WriteValueAsync` mit Typkonvertierung auf den BuiltInType des gecachten Werts (int → Double) sowie
string → NodeId/LocalizedText. Nicht beschreibbare Variablen und ungültige Werte führen zu
`OpenSmiInvalidValueException`, `BadUserAccessDenied` zu `OpenSmiAccessDeniedException`.
`ReadAllAsync`/`WriteAllAsync` laufen gebatcht; ein unbekannter Name wirft `KeyNotFoundException`,
bevor etwas geschrieben wird. Dazu `ValueChanged`.

## Akzeptanz (Integration)

- `DummyFloatParameter` ← `int 2` → gelesen `2.0`
- `WriteAllAsync({"Unbekannt": 1})` → `KeyNotFoundException`
- `ValueChanged` feuert nach dem Schreiben

## Answer

`WriteValueAsync` (Konvertierung per `TypeInfo.Cast`, Fallback über ValidValues) und `WriteAllAsync` (ein Request, abgelehnte Werte laufen erneut über den Einzel-Write). Tests: `WriteTests`.
