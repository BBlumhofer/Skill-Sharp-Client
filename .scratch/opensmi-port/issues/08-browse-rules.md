# 08 BrowseRules and matchers

Type: task
Status: resolved
Blocked by: 05

## Question

`[Flags] BrowseFeature`, `BrowseRule(Func<string,bool> Match, BrowseFeature Features)`,
`BrowseRules.When`, `BrowseRules.Evaluate`, `Match.All/Name/Path/PathPrefix/Any/AllOf/Not`,
`BrowseRule.Default`. Oracle: `client/browse.py`.

## Acceptance (unit)

- `Match.Name("DummyMachine")` matches `/DummyMachine`, but not `/DummyMachine/X`
- `PathPrefix("/A")` matches `/A` and `/A/B`, but not `/AB`
- `Evaluate` ORs the features of all matching rules; no match → `None`

## Answer

`Browse.cs`, unit tests `BrowseRulesTests` (example from `browse_rules.py`).
