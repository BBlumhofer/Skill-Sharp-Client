using System;
using System.Threading.Tasks;
using UAClient.Client;
using System.Linq;
using System.IO;
using System.Text.Json;

internal class Program
{
    private class Config
    {
        public string? Endpoint { get; set; }
        public string? Username { get; set; }
        public string? Password { get; set; }
        public string? ModuleName { get; set; }
    }

    private static async Task<int> Main(string[] args)
    {
        // Read config.json (default locations), or accept a config path as first arg
        string? configPath = args != null && args.Length > 0 ? args[0] : null;
        if (string.IsNullOrEmpty(configPath))
        {
            var candidates = new[] { "config.json", Path.Combine("Tools", "RemoteInspector", "config.json"), Path.Combine("Skill-Sharp-Client","Tools","RemoteInspector","config.json") };
            configPath = candidates.FirstOrDefault(File.Exists);
        }

        if (string.IsNullOrEmpty(configPath) || !File.Exists(configPath))
        {
            Console.WriteLine("Config file not found. Create a JSON config with Endpoint, Username, Password and ModuleName.");
            Console.WriteLine("Checked: ./config.json and ./Tools/RemoteInspector/config.json or pass path as first arg.");
            return 2;
        }

        Config? cfg = null;
        try
        {
            var json = await File.ReadAllTextAsync(configPath);
            cfg = JsonSerializer.Deserialize<Config>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to read/parse config: {ex.Message}");
            return 3;
        }

        if (cfg == null || string.IsNullOrEmpty(cfg.Endpoint))
        {
            Console.WriteLine("Config missing required field: Endpoint is required.");
            return 4;
        }

        var endpoint = cfg.Endpoint;
        var username = cfg.Username ?? string.Empty;
        var password = cfg.Password ?? string.Empty;

        Console.WriteLine($"=== Inspecting server: {endpoint} (module: {cfg.ModuleName}) ===");
        using var client = new UaClient(endpoint, username, password);
        var server = new RemoteServer(client);
        try
        {
            await server.ConnectAsync();
            Console.WriteLine($"Connected. Modules discovered: {server.Modules.Count}");


            // Determine which module to inspect. If not provided in config, pick the first module discovered.
            RemoteModule? module = null;
            if (!string.IsNullOrEmpty(cfg.ModuleName))
            {
                module = server.Modules.Values.FirstOrDefault(m => string.Equals(m.Name, cfg.ModuleName, StringComparison.OrdinalIgnoreCase));
                if (module == null)
                {
                    Console.WriteLine($"Module '{cfg.ModuleName}' not found on server. Available modules:");
                    foreach (var m in server.Modules.Values) Console.WriteLine($" - {m.Name}");
                    return 5;
                }
            }
            else
            {
                if (server.Modules == null || server.Modules.Count == 0)
                {
                    Console.WriteLine("No modules discovered on server.");
                    return 5;
                }
                module = server.Modules.Values.First();
                Console.WriteLine($"No ModuleName in config - using first discovered module: {module.Name}");
            }

            Console.WriteLine($"Module: {module.Name}");
            var neigh = module.Neighbors ?? new System.Collections.Generic.List<string>();
            Console.WriteLine(neigh.Count == 0
                ? "Neighbors: (none reported)"
                : $"Neighbors: {string.Join(", ", neigh)}");

            // Ports overview (include all discovered ports, not just coupled neighbors)
            if (module.Ports == null || module.Ports.Count == 0)
            {
                Console.WriteLine("Ports: (none discovered)");
            }
            else
            {
                Console.WriteLine("Ports:");
                foreach (var port in module.Ports.Values
                             .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase))
                {
                    bool isCoupled = false;
                    try
                    {
                        isCoupled = await port.IsCoupledAsync(client);
                    }
                    catch { }

                    var partnerTag = string.IsNullOrEmpty(port.PartnerRfidTag)
                        ? "(no partner)"
                        : port.PartnerRfidTag;

                    Console.WriteLine(
                        $" - {port.Name}: Coupled={isCoupled}, CoupleSkill={(PortSupportsCoupling(port) ? "yes" : "no")}, " +
                        $"Active={port.ActivePort}, PartnerTag={partnerTag}");
                }
            }

            // Closed ports
            var map = module.GetClosedPortsPartnerRfidTags();
            if (map == null || map.Count == 0)
            {
                Console.WriteLine("No closed ports with partner RFID tags found.");
            }
            else
            {
                Console.WriteLine("Closed ports with PartnerRfidTag:");
                foreach (var p in map)
                {
                    Console.WriteLine($"  Port '{p.Key}' -> '{p.Value}'");
                }
            }

            // Storage summary
            if (module.Storages == null || module.Storages.Count == 0)
            {
                Console.WriteLine("No storages discovered in this module.");
            }
            else
            {
                Console.WriteLine("Storage summary:");
                foreach (var sKv in module.Storages)
                {
                    var storage = sKv.Value;
                    try
                    {
                        var total = storage.Slots?.Count ?? 0;
                        var empty = storage.Slots?.Values.Count(sl => sl.IsSlotEmpty.HasValue && sl.IsSlotEmpty.Value) ?? 0;
                        var nonEmpty = storage.Slots?.Values.Count(sl => sl.IsSlotEmpty.HasValue && !sl.IsSlotEmpty.Value) ?? 0;
                        Console.WriteLine($" Storage: {storage.Name}  slots={total} nonEmpty={nonEmpty} empty={empty}");
                                // Print detailed per-slot information as requested
                                if (storage.Slots != null && storage.Slots.Count > 0)
                                {
                                    Console.WriteLine(" Slot details:");
                                    foreach (var slotKv in storage.Slots)
                                    {
                                        var slot = slotKv.Value;
                                        try
                                        {
                                            var carrierId = slot.CarrierId ?? "(null)";
                                            var productId = slot.ProductId ?? "(null)";
                                            var isSlotEmpty = slot.IsSlotEmpty.HasValue ? slot.IsSlotEmpty.Value.ToString() : "unknown";
                                            var isCarrierEmpty = slot.IsCarrierEmpty.HasValue ? slot.IsCarrierEmpty.Value.ToString() : "unknown";
                                            var carrierTypeDisplay = slot.CarrierTypeDisplay();
                                            var productTypeDisplay = slot.ProductTypeDisplay();
                                            Console.WriteLine($"  - {slot.Name}: CarrierID={carrierId} ProductID={productId} IsSlotEmpty={isSlotEmpty} IsCarrierEmpty={isCarrierEmpty} CarrierType={carrierTypeDisplay} ProductType={productTypeDisplay}");
                                        }
                                        catch (Exception exSlot)
                                        {
                                            Console.WriteLine($"   Error reading slot '{slotKv.Key}': {exSlot.Message}");
                                        }
                                    }
                                }
                    }
                    catch (Exception exStorage)
                    {
                        Console.WriteLine($"  Error summarizing storage {sKv.Key}: {exStorage.Message}");
                    }
                }
            }

            // Components
            Console.WriteLine("Components:");
            if (module.Components == null || module.Components.Count == 0) Console.WriteLine(" - (none)");
            else
            {
                foreach (var c in module.Components.Keys.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)) Console.WriteLine($" - {c}");
            }

            // Skills
            Console.WriteLine("Skills:");
            if (module.SkillSet == null || module.SkillSet.Count == 0) Console.WriteLine(" - (none)");
            else
            {
                foreach (var s in module.SkillSet.Keys.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)) Console.WriteLine($" - {s}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Connection/inspection failed: {ex.Message}");
            return 6;
        }
        finally
        {
            try { server.Dispose(); } catch { }
            try { client.Dispose(); } catch { }
        }

        return 0;
    }

    private static bool PortSupportsCoupling(RemotePort port)
    {
        try
        {
            if (port.SkillSet.Keys.Any(k => k.IndexOf("couple", StringComparison.OrdinalIgnoreCase) >= 0))
                return true;

            if (port.Methods.Values.OfType<RemoteSkill>().Any(IsCoupleSkill))
                return true;

            if (port.Module != null)
            {
                if (port.Module.Methods.Values.OfType<RemoteSkill>().Any(IsCoupleSkill))
                    return true;
                if (port.Module.SkillSet.Values.Any(IsCoupleSkill))
                    return true;
            }
        }
        catch
        {
            // ignore capability detection errors
        }

        return false;
    }

    private static bool IsCoupleSkill(RemoteSkill skill)
    {
        return skill.Name.IndexOf("couple", StringComparison.OrdinalIgnoreCase) >= 0;
    }
}
