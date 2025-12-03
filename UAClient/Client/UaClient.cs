using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Configuration;
using Opc.Ua.Client;

namespace UAClient.Client
{
    /// <summary>
    /// Lightweight wrapper around OPC UA .NET Standard Session for basic read/write/call operations.
    /// </summary>
    public class UaClient : IDisposable
    {
        private readonly string _url;
        private readonly string _username;
        private readonly string _password;
        private ApplicationConfiguration? _config;
        private Session? _session;

        public UaClient(string url, string username = "", string password = "")
        {
            _url = url;
            _username = username;
            _password = password;
        }

        public async Task ConnectAsync(uint sessionTimeout = 60000)
        {
            if (_session != null && _session.Connected)
                return;

            _config = new ApplicationConfiguration()
            {
                ApplicationName = "UAClient",
                ApplicationType = ApplicationType.Client,
                SecurityConfiguration = new SecurityConfiguration
                {
                    AutoAcceptUntrustedCertificates = true,
                    AddAppCertToTrustedStore = true,
                    TrustedIssuerCertificates = new CertificateTrustList { StoreType = "Directory", StorePath = "CertificateStores/UA_TrustedIssuerCertificates" },
                    TrustedPeerCertificates = new CertificateTrustList { StoreType = "Directory", StorePath = "CertificateStores/UA_TrustedPeerCertificates" },
                    RejectedCertificateStore = new CertificateTrustList { StoreType = "Directory", StorePath = "CertificateStores/UA_RejectedCertificates" }
                },
                TransportQuotas = new TransportQuotas { OperationTimeout = 15000 },
                ClientConfiguration = new ClientConfiguration()
            };

            // Configure where the application certificate will be stored and allow auto-accept
            _config.SecurityConfiguration.ApplicationCertificate = new CertificateIdentifier
            {
                StoreType = "Directory",
                StorePath = "CertificateStores/UA_MachineDefault",
                SubjectName = _config.ApplicationName
            };

            await _config.ValidateAsync(ApplicationType.Client, System.Threading.CancellationToken.None);

            // discover endpoints using a DiscoveryClient instance
            EndpointDescriptionCollection endpoints;
            using (var disco = DiscoveryClient.Create(new System.Uri(_url)))
            {
                endpoints = await disco.GetEndpointsAsync(null);
            }
            var endpoint = endpoints.OrderBy(e => e.SecurityLevel).LastOrDefault() ?? endpoints[0];
            UAClient.Common.Log.Info($"Selected endpoint: Url={endpoint.EndpointUrl}, SecurityPolicy={endpoint.SecurityPolicyUri}, SecurityMode={endpoint.SecurityMode}");
            try
            {
                if (endpoint.UserIdentityTokens != null)
                {
                    foreach (var t in endpoint.UserIdentityTokens)
                    {
                        UAClient.Common.Log.Info($"Endpoint UserTokenPolicy: TokenType={t.TokenType}, PolicyId={t.PolicyId}");
                    }
                }
            }
            catch { }
            var endpointConfig = EndpointConfiguration.Create(_config);
            var configuredEndpoint = new ConfiguredEndpoint(null, endpoint, endpointConfig);

            // Select an appropriate UserIdentity based on the endpoint's supported UserTokenPolicies
            UserIdentity identity;
            try
            {
                var tokens = endpoint.UserIdentityTokens;
                if (tokens != null && tokens.Any(t => t.TokenType == UserTokenType.Anonymous))
                {
                    identity = new UserIdentity();
                }
                else if (!string.IsNullOrEmpty(_username) && tokens != null && tokens.Any(t => t.TokenType == UserTokenType.UserName))
                {
                    identity = new UserIdentity(_username, _password);
                }
                else
                {
                    // Fallback: if username provided, prefer username token; otherwise try anonymous
                    if (!string.IsNullOrEmpty(_username)) identity = new UserIdentity(_username, _password);
                    else identity = new UserIdentity();
                }
            }
            catch
            {
                identity = string.IsNullOrEmpty(_username) ? new UserIdentity() : new UserIdentity(_username, _password);
            }

                // Create a session using the TAP API provided by the OPC UA .NET Standard library.
                // Prefer the Task-based overload with a ConfiguredEndpoint and await the result.
                // Create a session in a way that is compatible with multiple SDK versions.
                // Try to use a Task-based CreateAsync if available; otherwise call a matching Create overload via reflection.
                try
                {
                    _session = await CreateSessionCompatAsync(_config, configuredEndpoint, sessionTimeout, identity);
                }
                catch (Exception ex)
                {
                    // If session creation failed (often due to missing application certificate for secure endpoint),
                    // try a fallback to an endpoint with SecurityPolicy=None if available.
                    UAClient.Common.Log.Warn($"CreateSessionCompatAsync failed: {ex.Message}. Attempting fallback to SecurityPolicy=None.");
                    try
                    {
                        var noneEp = endpoints.FirstOrDefault(e => string.Equals(e.SecurityPolicyUri, Opc.Ua.SecurityPolicies.None, StringComparison.OrdinalIgnoreCase) && e.SecurityMode == MessageSecurityMode.None)
                                    ?? endpoints.FirstOrDefault(e => string.Equals(e.SecurityPolicyUri, Opc.Ua.SecurityPolicies.None, StringComparison.OrdinalIgnoreCase));
                        if (noneEp != null)
                        {
                            UAClient.Common.Log.Info($"Fallback endpoint: Url={noneEp.EndpointUrl}, SecurityPolicy={noneEp.SecurityPolicyUri}, SecurityMode={noneEp.SecurityMode}");
                            var endpointConfigNone = EndpointConfiguration.Create(_config);
                            var configuredEndpointNone = new ConfiguredEndpoint(null, noneEp, endpointConfigNone);
                            _session = await CreateSessionCompatAsync(_config, configuredEndpointNone, sessionTimeout, identity);
                        }
                        else
                        {
                            UAClient.Common.Log.Warn("No 'None' security endpoint found on server; rethrowing original exception.");
                            throw;
                        }
                    }
                    catch (Exception ex2)
                    {
                        UAClient.Common.Log.Warn($"Fallback attempt also failed: {ex2.Message}");
                        throw;
                    }
                }

            }

            private async Task<Session> CreateSessionCompatAsync(ApplicationConfiguration config, ConfiguredEndpoint configuredEndpoint, uint sessionTimeout, UserIdentity identity)
            {
                // Simplified: prefer the modern SessionFactory / DefaultSessionFactory CreateAsync API
                var asm = typeof(Session).Assembly;
                var sfType = asm.GetType("Opc.Ua.Client.DefaultSessionFactory") ?? asm.GetType("Opc.Ua.Client.SessionFactory");
                if (sfType == null)
                {
                    throw new InvalidOperationException("DefaultSessionFactory/SessionFactory type not found in OPC UA SDK assembly.");
                }

                // Try to obtain singleton instance via public static Instance property/field
                object? factory = null;
                try
                {
                    var prop = sfType.GetProperty("Instance", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.FlattenHierarchy);
                    if (prop != null) factory = prop.GetValue(null);
                    else
                    {
                        var field = sfType.GetField("Instance", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.FlattenHierarchy);
                        if (field != null) factory = field.GetValue(null);
                    }
                }
                catch { factory = null; }

                // If no singleton, attempt to create via parameterless constructor
                if (factory == null)
                {
                    try { factory = Activator.CreateInstance(sfType); } catch { factory = null; }
                }

                // Prefer instance CreateAsync; otherwise try static CreateAsync on the type.
                System.Reflection.MethodInfo? mi = null;
                bool invokeStatic = false;
                if (factory != null)
                {
                    mi = sfType.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                        .FirstOrDefault(m => m.Name == "CreateAsync" && m.GetParameters().Length >= 2 &&
                                             (m.GetParameters()[0].ParameterType == typeof(ApplicationConfiguration) || m.GetParameters()[0].ParameterType == typeof(Opc.Ua.ApplicationConfiguration)) &&
                                             m.GetParameters()[1].ParameterType == typeof(ConfiguredEndpoint));
                }
                if (mi == null)
                {
                    mi = sfType.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
                        .FirstOrDefault(m => m.Name == "CreateAsync" && m.GetParameters().Length >= 2 &&
                                             (m.GetParameters()[0].ParameterType == typeof(ApplicationConfiguration) || m.GetParameters()[0].ParameterType == typeof(Opc.Ua.ApplicationConfiguration)) &&
                                             m.GetParameters()[1].ParameterType == typeof(ConfiguredEndpoint));
                    if (mi != null) invokeStatic = true;
                }

                if (mi == null) throw new InvalidOperationException("SessionFactory.CreateAsync not found on DefaultSessionFactory/SessionFactory.");

                // Build parameter list matching the method signature, supplying sensible defaults
                var parms = mi.GetParameters();
                var args = new object?[parms.Length];
                for (int i = 0; i < parms.Length; i++)
                {
                    var ptype = parms[i].ParameterType;
                    if (ptype == typeof(ApplicationConfiguration) || ptype == typeof(Opc.Ua.ApplicationConfiguration)) args[i] = config;
                    else if (ptype == typeof(ConfiguredEndpoint)) args[i] = configuredEndpoint;
                    else if (ptype == typeof(bool)) args[i] = false;
                    else if (ptype == typeof(string)) args[i] = "UAClientSession";
                    else if (ptype == typeof(uint) || ptype == typeof(int)) args[i] = (object)sessionTimeout;
                    else if (ptype == typeof(UserIdentity) || ptype == typeof(IUserIdentity)) args[i] = identity;
                    else if (ptype == typeof(System.Threading.CancellationToken)) args[i] = System.Threading.CancellationToken.None;
                    else if (ptype == typeof(Opc.Ua.StringCollection) || ptype == typeof(System.Collections.Generic.IList<string>) || ptype == typeof(IList<string>)) args[i] = new List<string>();
                    else args[i] = null;
                }

                var taskObj = mi.Invoke(invokeStatic ? null : factory, args) as System.Threading.Tasks.Task;
                if (taskObj == null) throw new InvalidOperationException("SessionFactory.CreateAsync did not return a Task.");
                await taskObj.ConfigureAwait(false);
                var resultProp = taskObj.GetType().GetProperty("Result");
                var sessionObj = resultProp?.GetValue(taskObj);
                var session = sessionObj as Session ?? (sessionObj as ISession) as Session;
                if (session == null) throw new InvalidOperationException("SessionFactory.CreateAsync completed but did not return a Session.");

                // Ensure default subscription exists
                if (session.DefaultSubscription == null)
                {
                    var sub = new Subscription() { PublishingInterval = 1000 };
                    session.AddSubscription(sub);
                    await sub.CreateAsync();
                }

                return session;
            }

        public async Task DisconnectAsync()
        {
            if (_session != null)
            {
                try
                {
                    await _session.CloseAsync();
                }
                catch { }
                _session.Dispose();
                _session = null;
            }
        }

        public Session? Session => _session;

        public ApplicationConfiguration? Configuration => _config;

        public async Task<object?> ReadNodeAsync(string nodeId)
        {
            if (_session == null) throw new InvalidOperationException("Session not connected");
            var nid = new NodeId(nodeId);
            var dv = await _session.ReadValueAsync(nid, System.Threading.CancellationToken.None);
            return dv.Value;
        }

        public async Task WriteNodeAsync(string nodeId, object value)
        {
            if (_session == null) throw new InvalidOperationException("Session not connected");
            var nid = new NodeId(nodeId);
            var writeValue = new WriteValue
            {
                NodeId = nid,
                AttributeId = Attributes.Value,
                Value = new DataValue(new Variant(value))
            };
            var writeResponse = await _session.WriteAsync(null, new WriteValueCollection { writeValue }, System.Threading.CancellationToken.None);
            var results = writeResponse.Results;
            if (results != null && results.Count > 0 && StatusCode.IsBad(results[0]))
            {
                throw new ServiceResultException(results[0]);
            }
        }

        public async Task<object?> CallMethodAsync(string objectNodeId, string methodNodeId, params object[] inputs)
        {
            if (_session == null) throw new InvalidOperationException("Session not connected");
            var objectId = new NodeId(objectNodeId);
            var methodId = new NodeId(methodNodeId);
            try
            {
                // Prefer the TAP API CallAsync when available
                await _session.CallAsync(objectId, methodId, System.Threading.CancellationToken.None, inputs);
                return null;
            }
            catch
            {
                return null;
            }
        }

        public void Dispose()
        {
            try { DisconnectAsync().Wait(); } catch { }
        }
    }
}
