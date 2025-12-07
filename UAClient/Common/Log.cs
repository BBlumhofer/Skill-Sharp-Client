using System;

namespace UAClient.Common
{
    public static class Log
    {
        public enum LevelEnum { Error = 1, Warn = 2, Info = 3, Debug = 4 }

        // Current log level - can be controlled via environment variable 'UACLIENT_LOG_LEVEL'
        public static LevelEnum CurrentLevel { get; } = ParseLevel(Environment.GetEnvironmentVariable("UACLIENT_LOG_LEVEL")) ?? LevelEnum.Info;

        public static void Info(string msg) { if (ShouldLog(LevelEnum.Info)) Write("INFO", msg); }
        public static void Debug(string msg) { if (ShouldLog(LevelEnum.Debug)) Write("DEBUG", msg); }
        public static void Warn(string msg) { if (ShouldLog(LevelEnum.Warn)) Write("WARN", msg); }
        public static void Error(string msg) { if (ShouldLog(LevelEnum.Error)) Write("ERROR", msg); }

        private static bool ShouldLog(LevelEnum level) => (int)level <= (int)CurrentLevel;

        private static LevelEnum? ParseLevel(string? v)
        {
            if (string.IsNullOrEmpty(v)) return null;
            return v.Trim().ToUpperInvariant() switch
            {
                "ERROR" => LevelEnum.Error,
                "WARN" => LevelEnum.Warn,
                "WARNING" => LevelEnum.Warn,
                "INFO" => LevelEnum.Info,
                "INFORMATION" => LevelEnum.Info,
                "DEBUG" => LevelEnum.Debug,
                _ => null
            };
        }

        private static void Write(string level, string msg)
        {
            Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {level}: {msg}");
        }
    }
}
