# 01 Lage von `Requirements`

Type: research
Status: resolved
Blocked by: –

## Question

Liegt `Requirements` am Skill-Objekt oder unter `SkillExecution` (SPEC §6, offen)?

## Answer

Gegen den Dummy-Server gebrowst (2026-10-29): `CompositeDummySkill` hat `Requirements` an **beiden**
Stellen. Der Python-Client liest nur `SkillExecution/Requirements` und findet dort
`depends_on = [DummySkill.SkillExecution]`. Der C#-Port macht es genauso und sucht nur an dieser
einen Stelle. Die Suche „an beiden Stellen“ aus SPEC §6 entfällt.
