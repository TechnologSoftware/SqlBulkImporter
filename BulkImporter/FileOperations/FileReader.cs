using SqlFileImporter.ConsoleOutput;
using SqlFileImporter.DataClasses;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using static SqlFileImporter.Enums.Enums;

namespace SqlFileImporter.FileOperations
{
    /// <summary>
    /// The file reader to read the list of text file and to analyze the files regarding the content.
    /// </summary>
    internal class FileReader
    {
        readonly List<Exception> _exceptions = new List<Exception>();
        readonly Configuration _configuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileReader"/> class.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        internal FileReader(Configuration configuration) => _configuration = configuration;

        DataType ParseString(string str)
        {
            bool boolValue;
            int intValue;
            long bigintValue;
            double doubleValue;
            DateTime dateValue;

            // Place checks higher in if-else statement to give higher priority to type.
            if (bool.TryParse(str, out boolValue))
                return DataType.System_Boolean;
            else if (int.TryParse(str, out intValue))
                return DataType.System_Int32;
            else if (long.TryParse(str, out bigintValue))
                return DataType.System_Int64;
            else if (double.TryParse(str, out doubleValue))
                return DataType.System_Double;
            else if (DateTime.TryParse(str, out dateValue))
                return DataType.System_DateTime;
            else return DataType.System_String;
        }

        /// <summary>
        /// Gets the list of exceptions.
        /// </summary>
        /// <value>
        /// The exceptions list.
        /// </value>
        internal List<Exception> Exceptions => _exceptions;

        /// <summary>
        /// Gets the files.
        /// </summary>
        /// <param name="filePath">The file path.</param>
        /// <param name="ext">The ext.</param>
        /// <returns>The path list of all files to import.</returns>
        internal List<string> GetFiles(string filePath, string ext)
        {
            var importFiles = new List<string>();

            try
            {
                if (File.Exists(filePath))
                {
                    importFiles.Add(filePath);
                    ConsoleWriter.ConsoleLog($"File found: {filePath}", _configuration.LogPath, _configuration.IsSilent);
                }
                else if (File.Exists(Path.Combine(Environment.CurrentDirectory, filePath)))
                {
                    filePath = Path.Combine(Environment.CurrentDirectory, filePath);
                    importFiles.Add(filePath);
                    ConsoleWriter.ConsoleLog($"File found: {filePath}", _configuration.LogPath, _configuration.IsSilent);
                }
                else if (Directory.Exists(filePath))
                {
                    ConsoleWriter.ConsoleLog($"Search for files in {filePath}", _configuration.LogPath, _configuration.IsSilent);

                    var directoryInfo = new DirectoryInfo(filePath);
                    var fileInfo = directoryInfo.GetFiles(ext);

                    if (fileInfo.Count() == 0)
                    {
                        ConsoleWriter.ConsoleLog($"Search pattern '{ext}' found no files in folder: {filePath}", _configuration.LogPath, _configuration.IsSilent);
                    }
                    else
                    {
                        ConsoleWriter.ConsoleLog($"{fileInfo.Count()} files found.", _configuration.LogPath, _configuration.IsSilent);
                    }

                    foreach (FileInfo file in fileInfo)
                    {
                        importFiles.Add(file.FullName);
                        ConsoleWriter.ConsoleLog($"{file.Name}", _configuration.LogPath, _configuration.IsSilent);
                    }
                }
                else
                {
                    ConsoleWriter.ConsoleLog($"Path not found: {filePath}", _configuration.LogPath, _configuration.IsSilent);
                }
            }
            catch (Exception exception)
            {
                _exceptions.Add(exception);
                ConsoleWriter.ConsoleLog($"Error during directory reading.", _configuration.LogPath, _configuration.IsSilent);
                ConsoleWriter.ConsoleLog(exception.Message, _configuration.LogPath, _configuration.IsSilent);
            }

            if (!_configuration.IsSilent)
                Console.WriteLine("\r");

            return importFiles;
        }

        /// <summary>
        /// Reads the file.
        /// </summary>
        /// <param name="filePath">The file path.</param>
        /// <returns>The data table object.</returns>
        internal DataTable ReadFile(string filePath)
        {
            var dataTable = new DataTable($"[{_configuration.SchemaName}].[{_configuration.TablePrefix}{Path.GetFileNameWithoutExtension(filePath)}{_configuration.TableSuffix}]");
            var lines = File.ReadAllLines(filePath);
            var headers = lines.First().Split(new string[] { _configuration.ColumnDelimiter }, StringSplitOptions.None);
            var rows = lines.Skip(1).ToArray();

            ConsoleWriter.ConsoleLog($"Load file: {filePath}", _configuration.LogPath, _configuration.IsSilent);

            var dataColumns = new List<DataColumn>();

            foreach (string columnName in headers)
                dataColumns.Add(new DataColumn(columnName, typeof(bool)));

            var i = 1;
            var dataRowList = new List<string[]>();

            foreach (string row in rows)
            {
                var tupel = row.Split(new string[] { _configuration.ColumnDelimiter }, StringSplitOptions.None);
                dataRowList.Add(tupel);

                if (!_configuration.IsSilent)
                    Console.Write("\r{0} rows loaded.", i++);
            }

            if (!_configuration.IsSilent)
                Console.WriteLine("\r");

            for (int ordinal = 0; ordinal < dataColumns.Count; ordinal++)
            {
                i = 1;

                foreach (string[] row in dataRowList)
                {
                    if (ordinal <= (row.Count() - 1))
                    {
                        var valueType = ParseString(row[ordinal]);

                        if (valueType != DataType.System_Boolean && dataColumns[ordinal].DataType == typeof(bool))
                        {
                            // do nothing!
                        }

                        if (valueType == DataType.System_Int32 && dataColumns[ordinal].DataType == typeof(bool))
                        {
                            dataColumns[ordinal].DataType = typeof(int);
                        }

                        if (valueType == DataType.System_Int64 &&
                                 (
                                  dataColumns[ordinal].DataType == typeof(bool) ||
                                  dataColumns[ordinal].DataType == typeof(int)
                                 )
                                )
                        {
                            dataColumns[ordinal].DataType = typeof(long);
                        }

                        if (valueType == DataType.System_Double &&
                                 (
                                  dataColumns[ordinal].DataType == typeof(bool) ||
                                  dataColumns[ordinal].DataType == typeof(int) ||
                                  dataColumns[ordinal].DataType == typeof(long)
                                 )
                                )
                        {
                            dataColumns[ordinal].DataType = typeof(double);
                        }

                        if (valueType == DataType.System_DateTime &&
                                 (
                                  dataColumns[ordinal].DataType == typeof(bool) ||
                                  dataColumns[ordinal].DataType == typeof(int) ||
                                  dataColumns[ordinal].DataType == typeof(long) ||
                                  dataColumns[ordinal].DataType == typeof(double)
                                 )
                                )
                        {
                            dataColumns[ordinal].DataType = typeof(DateTime);
                        }

                        if (valueType == DataType.System_String &&
                                 (
                                  dataColumns[ordinal].DataType == typeof(bool) ||
                                  dataColumns[ordinal].DataType == typeof(int) ||
                                  dataColumns[ordinal].DataType == typeof(long) ||
                                  dataColumns[ordinal].DataType == typeof(double) ||
                                  dataColumns[ordinal].DataType == typeof(DateTime)
                                 )
                                )
                        {
                            dataColumns[ordinal].DataType = typeof(string);
                        }
                    }
                }
            }

            ConsoleWriter.ConsoleLog($"Data analysis done.", _configuration.LogPath, _configuration.IsSilent);

            foreach (DataColumn column in dataColumns)
                dataTable.Columns.Add(column);

            foreach (string[] row in dataRowList)
                dataTable.Rows.Add(row);

            dataColumns = null;
            dataRowList = null;

            GC.Collect();

            ConsoleWriter.ConsoleLog($"Batch created.", _configuration.LogPath, _configuration.IsSilent);

            return dataTable;
        }
    }
}