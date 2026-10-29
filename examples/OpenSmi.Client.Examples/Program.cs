using OpenSmi.Client.Examples;

// Usage: dotnet run -- <example> [url]
var name = args.ElementAtOrDefault(0) ?? "";
var url = args.ElementAtOrDefault(1) ?? (name == "quick_start" ? "opc.tcp://localhost:4841/" : "opc.tcp://localhost:4843/server");
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
var ct = cts.Token;

Func<Task>? example = name switch
{
    "auto_lock" => () => AutoLockExample.RunAsync(url, cancellationToken: ct),
    "browse_rules" => () => BrowseRulesExample.RunAsync(url, ct),
    "direct_component" => () => DirectComponentExample.RunAsync(url, ct),
    "dto" => () => DtoExample.RunAsync(url, ct),
    "example" => () => ColdStartExample.RunAsync(url, ct),
    "history" => () => HistoryExample.RunAsync(url, ct),
    "method_call" => () => MethodCallExample.RunAsync(url, ct),
    "quick_start" => () => QuickStartExample.RunAsync(url, cancellationToken: ct),
    "security_policy" => () => SecurityPolicyExample.RunAsync(url, cancellationToken: ct),
    "subscription_component" => () => SubscriptionComponentExample.RunAsync(url, cancellationToken: ct),
    "subscription_server" => () => SubscriptionServerExample.RunAsync(url, cancellationToken: ct),
    "subscription_variable" => () => SubscriptionVariableExample.RunAsync(url, cancellationToken: ct),
    _ => null,
};
if (example == null)
{
    Console.Error.WriteLine("Usage: dotnet run -- <auto_lock|browse_rules|direct_component|dto|example|history|method_call|" +
        "quick_start|security_policy|subscription_component|subscription_server|subscription_variable> [url]");
    return 1;
}
await example();
return 0;
