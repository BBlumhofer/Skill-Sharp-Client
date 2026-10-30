using Opc.Ua;

namespace OpenSmi.Client.Examples;

/// <summary>
/// Port of examples/security_policy.py: connect with Basic256Sha256/SignAndEncrypt and print every machine.
/// The client certificate is created once in <paramref name="certificateStorePath"/>; the server certificate must be in
/// <c>&lt;certificateStorePath&gt;/trusted/certs</c> unless <paramref name="autoAcceptServerCertificate"/> is set.
/// </summary>
public static class SecurityPolicyExample
{
    public static async Task RunAsync(string url, string certificateStorePath = "pki", bool autoAcceptServerCertificate = false,
        CancellationToken cancellationToken = default)
    {
        // remember: do not commit the actual password to version control...
        await using var server = await new RemoteServer(new()
        {
            Url = url, Username = "visitor", Password = "visitor",
            Security = new(SecurityPolicies.Basic256Sha256, MessageSecurityMode.SignAndEncrypt, certificateStorePath,
                autoAcceptServerCertificate),
        }).ConnectAsync(cancellationToken: cancellationToken);
        foreach (var machine in server.Machines.Values)
            Console.WriteLine(Print.Json(machine.ToDto()));
    }
}
