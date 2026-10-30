namespace OpenSmi.Client;

public enum RemoteServerStatus { Disconnected, Connecting, Browsing, Connected, Reconnecting, Disconnecting }

/// <summary>Skill states with the ids from opensmi-core <c>ua_enums.SkillState</c>.</summary>
public enum SkillState
{
    Halted = 11, Ready = 12, Running = 13, Suspended = 14, Completed = 15,
    Starting = 16, Halting = 17, Completing = 18, Resetting = 19, Suspending = 20,
}

public enum StartWait { None, RunningOrCompleted, Completion }

/// <summary>Integer results of the DI lock methods; bad status codes surface as exceptions instead.</summary>
public enum LockResult { Ok = 0, NotLocked = -1 }

[Flags]
public enum BrowseFeature
{
    None = 0,
    Machines = 1 << 0,
    Components = 1 << 1,
    Lock = 1 << 2,
    Resources = 1 << 3,
    Users = 1 << 4,
    SkillSet = 1 << 5,
    MethodSet = 1 << 6,
    Attributes = 1 << 7,
    Identification = 1 << 8,
    Monitoring = 1 << 9,
    ParameterSet = 1 << 10,
    FinalResultData = 1 << 11,
    All = (1 << 12) - 1,
}
