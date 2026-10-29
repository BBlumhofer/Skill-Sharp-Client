# 08 BrowseRules und Matcher

Type: task
Status: resolved
Blocked by: 05

## Question

`[Flags] BrowseFeature`, `BrowseRule(Func<string,bool> Match, BrowseFeature Features)`,
`BrowseRules.When`, `BrowseRules.Evaluate`, `Match.All/Name/Path/PathPrefix/Any/AllOf/Not`,
`BrowseRule.Default`. Orakel: `client/browse.py`.

## Akzeptanz (Unit)

- `Match.Name("DummyMachine")` trifft `/DummyMachine`, aber nicht `/DummyMachine/X`
- `PathPrefix("/A")` trifft `/A` und `/A/B`, aber nicht `/AB`
- `Evaluate` verodert die Features aller passenden Regeln; ohne Treffer → `None`

## Answer

`Browse.cs`, Unit-Tests `BrowseRulesTests` (Beispiel aus `browse_rules.py`).
