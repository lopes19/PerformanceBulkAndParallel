using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace PerformanceBulkAndParallel
{
    class Program
    {
        static IConfigurationRoot configuration;

        static void Main(string[] args)
        {
            BuildConfiguration();

            var source = GenerateRandomList();

            var dataTable = LoadDataTableInParallel(source);

            PersistInDatabase(dataTable);
        }

       
        private static void BuildConfiguration()
        {
            var builder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            configuration = builder.Build();
        }

        private static IEnumerable<Record> GenerateRandomList()
        {
            var recordCount = 1000;

            Console.WriteLine($"Quantidade de registros para a carga: {recordCount}");
            Console.WriteLine("");


            return Enumerable.Range(0, recordCount).Select(x => new Record());
        }

        private static DataTable LoadDataTableInParallel(IEnumerable<Record> source)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var dataTable = DataTableForBulkInsert.CreateDataTable();

            DataTableForBulkInsert.MergeListInDataTable(dataTable, source);

            stopwatch.Stop();

            Console.WriteLine($"Tempo para criar o DataTable e popular com o Parallel.ForEach: {stopwatch.Elapsed}");
            Console.WriteLine("");

            return dataTable;
        }

        private static void PersistInDatabase(DataTable dataTable)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var repository = new Repository(configuration.GetSection("DataContext:ConnectionString").Value);
            repository.InsertDataIntoSQLServerUsingSQLBulkCopy(dataTable);

            stopwatch.Stop();

            Console.WriteLine($"Tempo de persistência no banco usando tabela temporária e bulk insert: {stopwatch.Elapsed}");
            Console.WriteLine("");


            stopwatch = new Stopwatch();
            stopwatch.Start();

            var response = repository.MergeTempTableRecordsToMainTable();

            stopwatch.Stop();

            Console.WriteLine($"Tempo de processamento do merge de TempTable com MainTable: {stopwatch.Elapsed}");
            Console.WriteLine("");


        }

    }
}
