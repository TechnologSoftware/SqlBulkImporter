using NDesk.Options;
using System;
using System.IO;

namespace SqlFileImporter.ConsoleOutput
{
    /// <summary>
    /// Static class to write console and file log.
    /// </summary>
    internal static class ConsoleWriter
    {
        /// <summary>
        /// Write a header to the console.
        /// </summary>
        internal static void ConsoleHeader()
        {
            Console.WriteLine("\r");
            Console.WriteLine($"Dynamic flat file bulk loader for Microsoft SQL Server Databases");
            Console.WriteLine($"(C) 2009-2023 by Technolog.ch - http://www.technolog.ch");
            Console.WriteLine("\r");
        }

        /// <summary>
        /// Shows the help.
        /// </summary>
        /// <param name="p">The p.</param>
        internal static void ShowHelp(OptionSet p)
        {
            ConsoleHeader();
            Console.WriteLine("Usage  : BulkImporter [OPTIONS]");
            Console.WriteLine("");
            Console.WriteLine("CSV Example:");
            Console.WriteLine("bulkimporter.exe -s=\"S:\\src\\\" -d=\",\" -t=\"Server=Localhost\\SqlExpress; Initial Catalog=MyDatabase; Integrated Security=true;\"\n");

            Console.WriteLine("This program read flat file data and writes the data into a target\n" +
                "database. During the loading process, the data is analyzed and a value type\n" +
                "is assigned to the columns, according to the content. Before the data is loaded,\n" +
                "an existing target table is deleted and recreated with the correct column data\n" +
                "types.");

            Console.WriteLine();
            Console.WriteLine("Options and how to use them:\n");
            p.WriteOptionDescriptions(Console.Out);
        }

        /// <summary>
        /// Write content to the console and/or the log file.
        /// </summary>
        /// <param name="output">The output.</param>
        /// <param name="logPath">The log path.</param>
        /// <param name="IsSilent">if set to <c>true</c> [is silent].</param>
        internal static void ConsoleLog(object output, string logPath, bool IsSilent)
        {
            if (!IsSilent)
                Console.WriteLine(output);

            try
            {
                var path = Path.GetDirectoryName(logPath);
                var content = string.Empty;

                if (output.GetType() == typeof(string))
                {
                    content = $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} [INF] : {(string)output}";
                }
                else
                {
                    var exception = (Exception)output;
                    content = $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} [ERR] : {exception.ToString()}";
                }

                if (Directory.Exists(path) && !File.Exists(path))
                {
                    path = Path.Combine(path, DateTime.Now.ToString("yyyy-MM-dd") + "_BulkImporter.txt");
                }
                else if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                    Console.WriteLine($"Directory created: {path}");
                    path = Path.Combine(path, DateTime.Now.ToString("yyyy-MM-dd") + "_BulkImporter.txt");
                }

                using (StreamWriter sw = File.AppendText(path))
                    sw.WriteLine(content);
            }
            catch(Exception exception)
            {
                Console.WriteLine($"{exception.Message}");
                Environment.Exit(1);
            }
        }
    }
}
