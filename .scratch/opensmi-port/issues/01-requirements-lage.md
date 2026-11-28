# 01 Location of `Requirements`

Type: research
Status: resolved
Blocked by: none

## Question

Does `Requirements` sit on the skill object or under `SkillExecution` (SPEC §6, open)?

## Answer

Browsed against the dummy server (2026-10-29): `CompositeDummySkill` has `Requirements` in both
places. The Python client reads only `SkillExecution/Requirements` and finds
`depends_on = [DummySkill.SkillExecution]` there. The C# port does the same and searches only in that
one place. The search "in both places" from SPEC §6 is dropped.
