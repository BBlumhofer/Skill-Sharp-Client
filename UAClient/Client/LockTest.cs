using System;
using System.Threading.Tasks;
using Opc.Ua.Client;
using UAClient.Common;

namespace UAClient.Client
{
    public static class LockTest
    {
        public static async Task<int> Run(string url, string username, string password, string moduleName = "CA-Module")
        {
            var client = new UaClient(url, username, password);
            try
            {
                Log.Info("LockTest: connecting...");
                await client.ConnectAsync();
                Log.Info("LockTest: connected");

                var server = new RemoteServer(client);
                await server.ConnectAsync();
                Log.Info("LockTest: server connected and modules discovered");

                if (!server.Modules.TryGetValue(moduleName, out var module))
                {
                    Console.WriteLine($"Module '{moduleName}' not found. Available modules:");
                    foreach (var kv in server.Modules) Console.WriteLine($" - {kv.Key}");
                    return 2;
                }

                if (module.Lock == null)
                {
                    Console.WriteLine($"Module '{moduleName}' contains no Lock objects.");
                    return 3;
                }

                // use discovered Lock
                RemoteLock lockObj = module.Lock;

                Console.WriteLine($"Using lock object: {lockObj.Name} at {lockObj.BaseNodeId}");

                var session = client.Session ?? throw new InvalidOperationException("No session");

                Console.WriteLine("--- initial variable values ---");
                await lockObj.PrintVariablesAsync(session);

                Console.WriteLine("--- invoking InitLock ---");
                var initOk = await lockObj.InitLockAsync(session);
                Console.WriteLine($"InitLock call reported success={initOk}");

                // give a short moment for state propagation
                await Task.Delay(1000);

                Console.WriteLine("--- variables after InitLock ---");
                await lockObj.PrintVariablesAsync(session);

                Console.WriteLine("--- invoking Exit/ReleaseLock ---");
                var relOk = await lockObj.ReleaseLockAsync(session);
                Console.WriteLine($"ReleaseLock call reported success={relOk}");

                await Task.Delay(1000);
                Console.WriteLine("--- variables after ReleaseLock ---");
                await lockObj.PrintVariablesAsync(session);

                await client.DisconnectAsync();
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"LockTest failed: {ex}");
                try { await client.DisconnectAsync(); } catch { }
                return 1;
            }
        }
    }
}
