using Opc.Ua;

namespace OpenSmi.Client;

public class OpenSmiException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>Bad status code returned by a Read, Write or Call.</summary>
public class OpenSmiStatusException(StatusCode statusCode, string message) : OpenSmiException($"{message}: {statusCode}")
{
    public StatusCode StatusCode { get; } = statusCode;

    internal static OpenSmiStatusException From(StatusCode code, string context)
    {
        var c = code.Code;
        if (c == StatusCodes.BadUserAccessDenied || c == StatusCodes.BadRequiresLock)
            return new OpenSmiAccessDeniedException(code, context);
        if (c == StatusCodes.BadInvalidState) return new OpenSmiInvalidStateException(code, context);
        return new OpenSmiStatusException(code, context);
    }

    internal static void ThrowIfBad(StatusCode code, string context)
    {
        if (StatusCode.IsBad(code)) throw From(code, context);
    }
}

public sealed class OpenSmiAccessDeniedException(StatusCode code, string message) : OpenSmiStatusException(code, message);
public sealed class OpenSmiInvalidStateException(StatusCode code, string message) : OpenSmiStatusException(code, message);
public sealed class OpenSmiAuthenticationException(string message, Exception? inner = null) : OpenSmiException(message, inner);
public sealed class OpenSmiConnectionException(string message, Exception? inner = null) : OpenSmiException(message, inner);
public sealed class OpenSmiProtocolException(string message) : OpenSmiException(message);
public sealed class OpenSmiInvalidValueException(string message, Exception? inner = null) : OpenSmiException(message, inner);
public sealed class SkillHaltedException(string message) : OpenSmiException(message);
public sealed class SkillNotSuspendableException(string message) : OpenSmiException(message);
