using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace PerformanceBulkAndParallel
{
    public class DataTableForBulkInsert
    {
        public static DataTable CreateDataTable()
        {
            var dataTable = new DataTable();

            dataTable.Columns.Add(new DataColumn("Id", typeof(int)));
            dataTable.Columns.Add(new DataColumn("Description", typeof(string)));

            return dataTable;
        }

        public static void MergeListInDataTable(DataTable dataTable, IEnumerable<Record> recordList)
        {
            Parallel.ForEach(
                recordList,
                (record) =>
                {
                    lock (dataTable)
                    {
                        dataTable.Rows.Add(
                           record.Id,
                           record.Description
                        );
                    }
                });

        }
    }
}
