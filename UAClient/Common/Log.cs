using System;

namespace UAClient.Common
{
    public static class Log
    {
        public static void Info(string msg) => Write("INFO", msg);
        public static void Debug(string msg) => Write("DEBUG", msg);
        public static void Warn(string msg) => Write("WARN", msg);
        public static void Error(string msg) => Write("ERROR", msg);

        private static void Write(string level, string msg)
        {
            Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {level}: {msg}");
        }
    }
}
