using SqlFileImporter.ConsoleOutput;
using SqlFileImporter.DataClasses;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SqlFileImporter
{
    /// <summary>
    /// Data access class to bulk load the file into the database
    /// </summary>
    internal class DataAccess
    {
        readonly List<Exception> _exceptions = new List<Exception>();
        readonly Configuration _configuration;

        SqlConnection sqlConnection;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataAccess"/> class.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        internal DataAccess(Configuration configuration) => _configuration = configuration;

        /// <summary>
        /// Gets the exceptions.
        /// </summary>
        /// <value>
        /// The exceptions.
        /// </value>
        internal List<Exception> Exceptions => _exceptions;

        /// <summary>
        /// Connects the database.
        /// </summary>
        /// <returns>The name of the connected databse</returns>
        internal string ConnectDatabase()
        {
            try
            {
                if (_configuration.ConnectionString != string.Empty)
                {
                    sqlConnection = new SqlConnection(_configuration.ConnectionString);
                    ConsoleWriter.ConsoleLog($"Connect to...", _configuration.LogPath, _configuration.IsSilent);
                    ConsoleWriter.ConsoleLog($"Server   : {sqlConnection.DataSource}", _configuration.LogPath, _configuration.IsSilent);
                    ConsoleWriter.ConsoleLog($"Database : {sqlConnection.Database}", _configuration.LogPath, _configuration.IsSilent);
                    sqlConnection.Open();

                    ConsoleWriter.ConsoleLog($"Status   : {sqlConnection.State}", _configuration.LogPath, _configuration.IsSilent);

                    if (!_configuration.IsSilent)
                        Console.WriteLine("", _configuration.LogPath, _configuration.IsSilent);
                }
                else
                {
                    ConsoleWriter.ConsoleLog($"No connection string provided!", _configuration.LogPath, _configuration.IsSilent);
                    Environment.Exit(1);
                }

                if (sqlConnection.State != ConnectionState.Open)
                {
                    ConsoleWriter.ConsoleLog($"Failed to open database connection!", _configuration.LogPath, _configuration.IsSilent);
                    Environment.Exit(1);
                }
            }
            catch (SqlException sqlExceptio)
            {
                ConsoleWriter.ConsoleLog($"Unable to connect to the database with the provided connection string.", _configuration.LogPath, _configuration.IsSilent);
                ConsoleWriter.ConsoleLog($"SQLException: {sqlExceptio.Message}", _configuration.LogPath, _configuration.IsSilent);
                Environment.Exit(1);
            }
            catch (Exception exception)
            {
                ConsoleWriter.ConsoleLog($"Exception: {exception.Message}", _configuration.LogPath, _configuration.IsSilent);
                Environment.Exit(1);
            }

            return sqlConnection?.Database;
        }

        /// <summary>
        /// Drops the and create target table.
        /// </summary>
        /// <param name="dataTable">The datatable.</param>
        void DropAndCreateTargetTable(DataTable dataTable)
        {
            var sqlScript = CreateDropCreateCommand(dataTable);

            try
            {
                ConsoleWriter.ConsoleLog($"DROP and CREATE TABLE {dataTable.TableName}", _configuration.LogPath, _configuration.IsSilent);
                var sqlCommand = new SqlCommand(sqlScript, sqlConnection);
                sqlCommand.ExecuteNonQuery();
            }
            catch (SqlException sqlException)
            {
                _exceptions.Add(sqlException);
                ConsoleWriter.ConsoleLog($"Error during creation of table {dataTable.TableName}", _configuration.LogPath, _configuration.IsSilent);
                ConsoleWriter.ConsoleLog(sqlException.Message, _configuration.LogPath, _configuration.IsSilent);
            }
            catch (Exception exception)
            {
                _exceptions.Add(exception);
                ConsoleWriter.ConsoleLog(exception.Message, _configuration.LogPath, _configuration.IsSilent);
            }
        }

        /// <summary>
        /// Creates the drop create command.
        /// </summary>
        /// <param name="dataTable">The data table.</param>
        /// <returns></returns>
        string CreateDropCreateCommand(DataTable dataTable)
        {
            var sqlScript = "";
            sqlScript += $"IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{dataTable.TableName}') AND type in (N'U'))";
            sqlScript += $"DROP TABLE {dataTable.TableName}; ";
            sqlScript += $"CREATE TABLE {dataTable.TableName}";
            sqlScript += "(";

            for (int i = 0; i < dataTable.Columns.Count; i++)
            {
                string columnType;

                switch (dataTable.Columns[i].DataType.Name.ToLower())
                {
                    case "int32":
                        columnType = "INT";
                        break;
                    case "int64":
                        columnType = "BIGINT";
                        break;
                    case "double":
                        columnType = "FLOAT";
                        break;
                    case "datetime":
                        columnType = "DATETIME";
                        break;
                    default:
                        columnType = "NVARCHAR(MAX)";
                        break;
                }

                if (i != dataTable.Columns.Count - 1)
                    sqlScript += dataTable.Columns[i].ColumnName + " " + columnType + ",";
                else
                    sqlScript += dataTable.Columns[i].ColumnName + " " + columnType;
            }

            sqlScript += ")";

            return sqlScript;
        }

        /// <summary>
        /// Bulks the insert data table.
        /// </summary>
        /// <param name="dataTable">The data table.</param>
        internal void BulkInsertDataTable(DataTable dataTable)
        {
            DropAndCreateTargetTable(dataTable);

            ConsoleWriter.ConsoleLog($"Writing {dataTable.Rows.Count} records to the database table {dataTable.TableName}.", _configuration.LogPath, _configuration.IsSilent);

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection))
            {
                foreach (DataColumn c in dataTable.Columns)
                    bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);

                bulkCopy.DestinationTableName = $"{dataTable.TableName}";

                try
                {
                    bulkCopy.WriteToServer(dataTable);
                }
                catch (Exception exception)
                {
                    _exceptions.Add(exception);
                    ConsoleWriter.ConsoleLog($"Error during bulk insert into table {dataTable.TableName}.", _configuration.LogPath, _configuration.IsSilent);
                    ConsoleWriter.ConsoleLog(exception.Message, _configuration.LogPath, _configuration.IsSilent);
                }
            }

            ConsoleWriter.ConsoleLog($"Import to table {dataTable.TableName} done.", _configuration.LogPath, _configuration.IsSilent);

            if (!_configuration.IsSilent)
                Console.WriteLine("\r");
        }

        /// <summary>
        /// Bulks the insert data table with transaction.
        /// </summary>
        /// <param name="dataTable">The data table.</param>
        internal void BulkInsertDataTableWithTransaction(DataTable dataTable)
        {
            DropAndCreateTargetTable(dataTable);

            ConsoleWriter.ConsoleLog($"Writing {dataTable.Rows.Count} records to the database table {dataTable.TableName}.", _configuration.LogPath, _configuration.IsSilent);

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection.ConnectionString, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction))
            {
                foreach (DataColumn c in dataTable.Columns)
                    bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);

                bulkCopy.DestinationTableName = $"{dataTable.TableName}";

                try
                {
                    bulkCopy.WriteToServer(dataTable);
                }
                catch (Exception exception)
                {
                    _exceptions.Add(exception);
                    ConsoleWriter.ConsoleLog($"Error during bulk insert into table {dataTable.TableName}.", _configuration.LogPath, _configuration.IsSilent);
                    ConsoleWriter.ConsoleLog(exception.Message, _configuration.LogPath, _configuration.IsSilent);
                }
            }

            ConsoleWriter.ConsoleLog($"Import to table {dataTable.TableName} done.", _configuration.LogPath, _configuration.IsSilent);
            Console.WriteLine("\r");
        }
    }
}
