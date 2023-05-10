using SqlFileImporter.ConsoleOutput;
using NDesk.Options;
using System;

namespace SqlFileImporter.DataClasses
{
    /// <summary>
    /// Configuration class with settings from the command line arguments
    /// </summary>
    internal class Configuration
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Configuration"/> class.
        /// </summary>
        /// <param name="args">The arguments.</param>
        internal Configuration(string[] args) => CreateConfigurationFromArguments(args);

        internal string ConnectionString { get; set; }
        internal string FileFolder { get; set; }
        internal string FileExtension { get; set; }
        internal string LogPath { get; set; } = Environment.CurrentDirectory;
        internal string ColumnDelimiter { get; set; } = ",";
        internal string SchemaName { get; set; } = "dbo";
        internal string TablePrefix { get; set; }
        internal string TableSuffix { get; set; }
        internal bool UseTransaction { get; set; }
        internal bool IsSilent { get; set; }
        internal bool ShowHelp { get; set; }

        /// <summary>
        /// Gets or sets the options.
        /// </summary>
        /// <value>
        /// The options.
        /// </value>
        internal OptionSet Options { get; set; }

        /// <summary>
        /// Creates the configuration from arguments.
        /// </summary>
        /// <param name="args">The arguments.</param>
        void CreateConfigurationFromArguments(string[] args)
        {
            Options = new OptionSet() {
            { "s|source=", "The path of the folder to search for files, or a specifit file to import.\n\nATTENTION: Don't use trailing \"\\\" !\n\n" +
            "Load a directory  -s=\"S:\\Stage Files\" \n" +
            "Load a file       -s=\"S:\\Stage Files\\MyData.csv\"\n", path => FileFolder = path },

            { "f|find=", "The search pattern to find files in a directory\n\n" +
            "-f=\"*.csv\"  or -f=\"stage*.csv\"\n", pattern => FileExtension = pattern},

            { "t|target=", "The connection string to the MS SQL Server database, including the database to whick the data is to be written.\n\n" +
            "-t=\"Server=Localhost\\SqlExpress; Initial Catalog=MyDatabase; Integrated Security=true;\"\n", value => ConnectionString = value },

            { "d|delimiter=", "The delimiter to parse the content of the file.\n\n" +
            "-d=\"||\"  or -d=\";\"\n", splitter => ColumnDelimiter = splitter},

            { "sc|schema=", "The name of the database schema (Default: dbo).\n\n" +
            "-sc=\"stagedb\"  ([stagedb].[TableName])\n", name => SchemaName = name},

            { "pr|prefix=", "The table prefix to set before the table name.\n\n" +
            "-pr=\"Import_\"  ([dbo].[Import_TableName])\n", text => TablePrefix = text},

            { "su|suffix=", "The table suffix to set after the table name.\n\n" +
            "-su=\"_Import\"  ([dbo].[TableName_Import])\n", text => TableSuffix = text},

            { "tran|transaction", "The switch to use database transaction safety. Default setting is false.\n", value => UseTransaction = value != null},

            { "l|log=",  "The path and file name where the logfile should be written. If this option is missing, there will no logfile be written.\n\n" +
            "-l=\"C:\\Logs\\BulkImport.txt\"\n", path => LogPath = path  },

            { "sil|silent",  "The program do not produce any console output.\n\n", value => IsSilent = value != null },

            { "?|h|help",  "Show this message and exit.\n", value => ShowHelp = value != null },
            };

            try
            {
                Options.Parse(args);
            }
            catch (Exception exception)
            {
                ConsoleWriter.ConsoleLog(exception.Message, LogPath, false);
                Environment.Exit(1);
            }
        }
    }
}
