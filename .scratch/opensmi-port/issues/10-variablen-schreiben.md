# 10 Writing variables, ReadAll/WriteAll, ValueChanged

Type: task
Status: resolved
Blocked by: 09, 13

## Question

`WriteValueAsync` with type conversion to the BuiltInType of the cached value (int → Double) and
string → NodeId/LocalizedText. Variables that are not writable and invalid values lead to
`OpenSmiInvalidValueException`, `BadUserAccessDenied` to `OpenSmiAccessDeniedException`.
`ReadAllAsync`/`WriteAllAsync` run batched; an unknown name throws `KeyNotFoundException`
before anything is written. Plus `ValueChanged`.

## Acceptance (integration)

- `DummyFloatParameter` ← `int 2` → read back as `2.0`
- `WriteAllAsync({"Unknown": 1})` → `KeyNotFoundException`
- `ValueChanged` fires after the write

## Answer

`WriteValueAsync` (conversion via `TypeInfo.Cast`, fallback via ValidValues) and `WriteAllAsync` (one request; rejected values go through the single write again). Tests: `WriteTests`.
