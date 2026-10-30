using Opc.Ua;

namespace OpenSmi.Client;

/// <summary>
/// Message security of the connection. With security the client certificate is created once in
/// <see cref="CertificateStorePath"/> and reused; server certificates must be in the trust store
/// (default <c>&lt;CertificateStorePath&gt;/trusted</c>) unless <see cref="AutoAcceptServerCertificate"/> is set.
/// </summary>
public sealed record SecurityOptions(
    string SecurityPolicyUri,
    MessageSecurityMode Mode,
    string? CertificateStorePath,
    bool AutoAcceptServerCertificate = false,
    string? TrustedPeerStorePath = null)
{
    public static SecurityOptions None { get; } = new(SecurityPolicies.None, MessageSecurityMode.None, null);

    internal bool IsNone => Mode == MessageSecurityMode.None;
}
