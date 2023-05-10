using SqlFileImporter.ConsoleOutput;
using SqlFileImporter.DataClasses;
using System;
using System.Diagnostics;

namespace SqlFileImporter
{
    /// <summary>
    /// Main entry point for the consoles application
    /// </summary>
    class Program
    {
        static Configuration AppConfiguration;
        static ImportWorkflow Workflow;
        static Stopwatch stopWatch = new Stopwatch();

        public static void Main(string[] args)
        {
            stopWatch.Start();

            if(args.Length == 0 )
            {
                args = new string[] { "-?" };
            }

            AppConfiguration = new Configuration(args);

            if (!AppConfiguration.IsSilent)
            {
                ConsoleWriter.ConsoleHeader();
            }

            if (AppConfiguration.ShowHelp)
            {
                ConsoleWriter.ShowHelp(AppConfiguration.Options);
                return;
            }

            Workflow = new ImportWorkflow(AppConfiguration, new FileOperations.FileReader(AppConfiguration), new DataAccess(AppConfiguration));

            Workflow.RunFileImport();

            stopWatch.Stop();

            ConsoleWriter.ConsoleLog($"Imported {Workflow.ImportCounter} tables into database {Workflow.DatabaseName}.", AppConfiguration.LogPath, AppConfiguration.IsSilent);
            ConsoleWriter.ConsoleLog("Data import completed!", AppConfiguration.LogPath, AppConfiguration.IsSilent);
            ConsoleWriter.ConsoleLog("\r", AppConfiguration.LogPath, AppConfiguration.IsSilent);
            ConsoleWriter.ConsoleLog($"Execution Time: {stopWatch.Elapsed}.", AppConfiguration.LogPath, AppConfiguration.IsSilent);

            if (Workflow.Exceptions.Count == 0)
                Environment.Exit(0);
            else
                Environment.Exit(1);
        }
        
    }
}