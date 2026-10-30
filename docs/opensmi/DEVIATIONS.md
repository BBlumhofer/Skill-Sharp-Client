# Abweichungen vom Python-Client

Der C#-Client verhält sich wie `opensmi-client @ e8740c2`, außer an den Stellen in dieser Tabelle.
Die ersten elf Zeilen stammen aus [SPEC §10](SPEC.md#10-bewusste-abweichungen-vom-python-original-zusammenfassung)
(die Security-Zeile der SPEC ist hier zweigeteilt), die letzten vier aus späteren Entscheidungen (Tickets 25, 26 und 30 unter `.scratch/opensmi-port/issues`).
Jede Zeile nennt die Stelle im Code und den Test, der das Verhalten belegt; alle Tests liegen in
`tests/OpenSmi.Client.Tests`. Behebt upstream einen der Fehler, wird das Verhalten angeglichen.

Hinweis: Dieses Dokument wurde mit KI-Unterstützung (Claude, Anthropic) erstellt; siehe den KI-Hinweis im README.

| Python | C# | Code | Test |
|---|---|---|---|
| `start(wait=True)` wartet nur auf Running/Completed | `StartWait.Completion` wartet auf Completed, bei kontinuierlichen Skills `InvalidOperationException` | `Enums.cs` (`StartWait`), `RemoteCallable.cs` (`RemoteSkill.StartAsync`) | `SkillTests.Finite_skill_runs_to_completion`, `SkillTests.Completion_wait_is_rejected_for_continuous_skills` |
| Waits ohne Timeout | jedes Wait hat `timeout` (`TimeoutException`) und `CancellationToken` | `RemoteCallable.cs` (`RemoteSkill.WaitForStatesAsync`) | `DeviationTests.Skill_wait_ends_with_timeout_or_cancellation_instead_of_hanging` |
| `ua_value_rank` immer 0 | ValueRank wird vom Server gelesen | `RemoteVariable.cs` (`UaValueRank`) | `BrowseModelTests.Variables_carry_metadata_like_the_python_client` |
| `RemoteUserDTO.current_access_level` ist die Lock-Priorität | heißt `LockPriority` | `RemoteUser.cs` (`LockPriority`), `Dto.cs` (`RemoteUserDto`) | `SkillTests.Users_expose_lock_priority`, `DtoTests.Machine_dto_has_the_same_objects_as_the_python_client` |
| `RemoteMachine.dto()` wirft `AssertionError`, wenn der Lock nicht gebrowst wurde | `Lock = null` im DTO | `Dto.cs` (`ComponentDto`) | `DtoTests.Machine_dto_without_browsed_lock_has_no_lock` |
| `read_current_state` wirft `KeyError` bei unbekanntem Zustand | `OpenSmiProtocolException` | `RemoteCallable.cs` (`RemoteSkill.ParseState`) | `SkillStateParsingTests.Unknown_state_text_is_a_protocol_error` |
| Rückgabewerte der Lock-Methoden werden verworfen | `LockResult` (`Ok`, `NotLocked`) | `RemoteLock.cs` (`InitLockAsync` usw.), `Enums.cs` (`LockResult`) | `LockTests.Init_and_exit_lock` |
| neues Client-Zertifikat bei jedem Connect | Zertifikat wird einmal im Store angelegt und wiederverwendet | `SecurityOptions.cs` (`CertificateStorePath`), `RemoteServer.cs` (Security-Konfiguration) | `SecurityTests.Client_certificate_is_created_once_and_reused` |
| Server-Zertifikat wird nicht geprüft, kein Trust-Store | Prüfung gegen den Trust-Store, `AutoAcceptServerCertificate` muss man ausdrücklich setzen | `SecurityOptions.cs` (`AutoAcceptServerCertificate`, `TrustedPeerStorePath`), `RemoteServer.cs` | `SecurityTests.Untrusted_server_certificate_fails_without_retry`, `SecurityTests.Server_certificate_in_trust_store_connects_without_auto_accept`, `SecurityTests.Encrypted_connect_with_auto_accept_browses_the_machine` |
| `read_all`/`write_all` lesen und schreiben einzeln nacheinander | ein Read- bzw. Write-Request; abgelehnte Werte laufen einzeln über `WriteValueAsync` | `RemoteVariableContainer.cs` (`ReadAllAsync`, `WriteAllAsync`) | `WriteTests.Write_all_writes_a_batch_and_rejects_unknown_names_before_writing` |
| `feasibility_check_set`/`precondition_check_set` fehlen im DTO | sind im DTO enthalten | `Dto.cs` (`ComponentDto`) | `DtoTests.Machine_dto_has_the_same_objects_as_the_python_client` |
| `Signal` mit `connect(handler)`, Handler laufen nebenläufig (`gather`) | `event AsyncEventHandler<XxxEventArgs>` (`+=`/`-=`), Handler laufen nacheinander in Anmeldereihenfolge, Fehler je Handler geloggt; dazu `WatchStateAsync`/`WatchValueAsync`/`WatchStatusAsync` als `IAsyncEnumerable` (erst der aktuelle Wert, kleiner Puffer, älteste Werte fallen weg) | `AsyncEvents.cs`, `RemoteCallable.cs` (`RemoteSkill.WatchStateAsync`), `RemoteVariable.cs` (`WatchValueAsync`), `RemoteServer.cs` (`WatchStatusAsync`) | `EventTests`, `SkillTests.Watch_state_sees_running_and_completed` |
| Unit-Enum in Großbuchstaben (`CENTIMETRE`) | PascalCase (`Unit.Centimetre`); `EUInformation.Description` bleibt der Python-Name | `tools/generate_units.py`, `Unit.g.cs`, `Units.cs` | `UnitTests.Eu_information_round_trips_like_python` |
| Custom-Datentypen: `load_data_type_definitions` beim Connect (global, Enums und Structs), Enum-Wert als `IntEnum`-Member (JSON: Zahl) | Je Variable mit eigenem DataType wird `DataTypeDefinition` gelesen, nur Enums: `ValidValues` = `"0"` → Name, `Value` = Membername (`"Green"`); Structs bleiben `ExtensionObject`; Lesefehler nur Warnung | `RemoteVariable.cs` (`InitCoreAsync`) | `CustomTypeTests.Enum_variable_exposes_type_name_valid_values_and_member_name` |
| Schreiben eines Enum-Membernamens scheitert (asyncua-Serialisierungsfehler, nicht abgefangen); Anzeigename wird erst nach Ablehnung als Schlüssel neu geschrieben | Ein Anzeigename aus `ValidValues` wird vor dem Schreiben auf seinen Schlüssel abgebildet (gilt auch für `WriteAllAsync`); nicht konvertierbarer Wert → `OpenSmiInvalidValueException` | `RemoteVariable.cs` (`ToWriteValue`) | `CustomTypeTests.Enum_variable_is_written_by_member_name_or_number` |
