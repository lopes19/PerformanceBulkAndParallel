using Dapper;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace PerformanceBulkAndParallel
{
    public  class Repository
    {
        private readonly string _connectionString;

        public Repository(string connectionString)
        {
            this._connectionString = connectionString;
        }

        public void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable dataTable)
        {
            var destinationTableName = "TempTable";

            using (var dbConnection = new SqlConnection(_connectionString))
            {
                dbConnection.Open();

                var sqlTrunc = "TRUNCATE TABLE " + destinationTableName;
                var cmd = new SqlCommand(sqlTrunc, dbConnection);
                cmd.ExecuteNonQuery();


                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null))
                {
                    s.DestinationTableName = destinationTableName;

                    foreach (var column in dataTable.Columns)
                        s.ColumnMappings.Add(column.ToString(), column.ToString());

                    s.WriteToServer(dataTable);
                }
            }
        }


        public ResultMessage MergeTempTableRecordsToMainTable()
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                var result = conn.Query<ResultMessage>
                (
                    "sp_MergeTempTableRecordsToMainTable",
                    new
                    {

                    },
                    commandType: System.Data.CommandType.StoredProcedure
                );

                return result.FirstOrDefault();
            }
        }
    }
}
