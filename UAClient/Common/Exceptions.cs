using System;

namespace UAClient.Common
{
    public class PyUaAdapterError : Exception
    {
        public PyUaAdapterError(string? message = null) : base(message) { }
    }

    public class SkillHaltedError : PyUaAdapterError { public SkillHaltedError(string? m = null) : base(m) { } }
    public class UnsupportedSkillNodeSetError : PyUaAdapterError { public UnsupportedSkillNodeSetError(string? m = null) : base(m) { } }
    public class SkillNotSuspendableError : PyUaAdapterError { public SkillNotSuspendableError(string? m = null) : base(m) { } }

    public class PyUaRuntimeError : PyUaAdapterError
    {
        public string? ErrorCode { get; }
        public PyUaRuntimeError(string msg, string errorCode = "") : base(msg) { ErrorCode = errorCode; }
    }
}
