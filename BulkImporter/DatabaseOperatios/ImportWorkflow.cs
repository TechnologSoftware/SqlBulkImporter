using SqlFileImporter.DataClasses;
using SqlFileImporter.FileOperations;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlFileImporter
{
    /// <summary>
    /// The workflow of the file importer.
    /// </summary>
    internal class ImportWorkflow
    {
        readonly List<Exception> _exceptions = new List<Exception>();
        readonly Configuration _configuration;
        readonly FileReader _reader;
        readonly DataAccess _dataAccess;

        /// <summary>
        /// Gets or sets the import counter for the processed files.
        /// </summary>
        /// <value>
        /// The number of the imported files.
        /// </value>
        internal int ImportCounter { get; set; }

        /// <summary>
        /// Gets or sets the name of the database.
        /// </summary>
        /// <value>
        /// The name of the database.
        /// </value>
        internal string DatabaseName { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ImportWorkflow"/> class.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        /// <param name="fileReader">The file reader.</param>
        /// <param name="dataAccess">The data access.</param>
        internal ImportWorkflow(Configuration configuration, FileReader fileReader, DataAccess dataAccess)
        {
            _configuration = configuration;
            _reader = fileReader;
            _dataAccess = dataAccess;
        }

        /// <summary>
        /// Gets the list of exceptions.
        /// </summary>
        /// <value>
        /// The exceptions.
        /// </value>
        internal List<Exception> Exceptions => _exceptions;

        /// <summary>
        /// Runs the file import.
        /// </summary>
        internal void RunFileImport()
        {
            var listOfFiles = _reader.GetFiles(_configuration.FileFolder, _configuration.FileExtension);

            if (listOfFiles.Any())
            {
                _dataAccess.ConnectDatabase();
            }

            foreach (var file in listOfFiles)
            {
                switch (_configuration.UseTransaction)
                {
                    case true:
                        _dataAccess.BulkInsertDataTableWithTransaction(_reader.ReadFile(file));
                        break;

                    case false:
                        _dataAccess.BulkInsertDataTable(_reader.ReadFile(file));
                        break;
                }
            }

            _exceptions.Concat(_reader.Exceptions);
            _exceptions.Concat(_dataAccess.Exceptions);

            ImportCounter = listOfFiles.Count();
        }
    }
}
