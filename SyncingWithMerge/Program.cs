using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using System.Collections.Generic;

namespace ETLBoxDemo.SyncingWithMerge
{
    class Program
    {
        static string ConnectionString = @"Data Source=localhost;Initial Catalog=demo;Integrated Security=false;User=sa;password=YourStrong@Passw0rd";
        static SqlConnectionManager SqlConnection { get; set; }
        static void Main(string[] args)
        {
            //SqlCon
            SqlConnection = new SqlConnectionManager(ConnectionString);
            RecreateDatabase("demo", ConnectionString);
            Prepare();

            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(SqlConnection, "source");
            DbMerge<MyMergeRow> merge = new DbMerge<MyMergeRow>(SqlConnection, "destination");
            source.LinkTo(merge);
            source.Execute();
            merge.Wait();
        }

        public static void RecreateDatabase(string dbName, SqlConnectionString connectionString)
        {
            var masterConnection = new SqlConnectionManager(connectionString.CloneWithMasterDbName());
            DropDatabaseTask.DropIfExists(masterConnection, dbName);
            CreateDatabaseTask.Create(masterConnection, dbName);
        }

        public static void Prepare()
        {
            TableDefinition SourceTableDef = new TableDefinition("source",
                new List<TableColumn>() {
                    new TableColumn("Key", "int",allowNulls: false, isPrimaryKey:true),
                    new TableColumn("Value","nvarchar(100)", allowNulls: false)
            });

            TableDefinition DestinationTableDef = new TableDefinition("destination",
                new List<TableColumn>() {
                    new TableColumn("Key", "int",allowNulls: false, isPrimaryKey:true),
                    new TableColumn("Value","nvarchar(100)", allowNulls: false)
            });

            CreateTableTask.Create(SqlConnection, SourceTableDef);
            CreateTableTask.Create(SqlConnection, DestinationTableDef);
            //Create demo tables & fill with demo data
            SqlTask.ExecuteNonQuery(SqlConnection, "Fill source table",
                "INSERT INTO source VALUES (1, 'Test - Insert')");
            SqlTask.ExecuteNonQuery(SqlConnection, "Fill source table",
                "INSERT INTO source VALUES (2, 'Test - Update')");
            SqlTask.ExecuteNonQuery(SqlConnection, "Fill source table",
                "INSERT INTO source VALUES (3, 'Test - Exists')");

            SqlTask.ExecuteNonQuery(SqlConnection, "Fill destination table",
                "INSERT INTO destination VALUES (2, 'XXX')");
            SqlTask.ExecuteNonQuery(SqlConnection, "Fill source table",
                "INSERT INTO destination VALUES (3, 'Test - Exists')");
            SqlTask.ExecuteNonQuery(SqlConnection, "Fill source table",
                "INSERT INTO destination VALUES (4, 'Test - Deleted')");
        }
    }

    public class MyMergeRow : MergeableRow
    {
        [IdColumn]
        public int Key { get; set; }

        [CompareColumn]
        public string Value { get; set; }
    }

}
