using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using System.Collections.Generic;

namespace ETLBoxDemo.SyncingWithMerge
{
    class Program
    {
        static string ConnectionString = @"Data Source=localhost;Initial Catalog=demo;Integrated Security=false;User=sa;password=YourStrong@Passw0rd;TrustServerCertificate=true";
        static SqlConnectionManager SqlConnection { get; set; }
        static void Main(string[] args) {
            //Connect Logger 
            InitializeLoggingWithNlog();

            //Prepare database
            SqlConnection = new SqlConnectionManager(ConnectionString);
            RecreateDatabase("demo", ConnectionString);
            Prepare();

            //Execute example
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(SqlConnection, "source");
            DbMerge<MyMergeRow> merge = new DbMerge<MyMergeRow>(SqlConnection, "destination");
            source.LinkTo(merge);
            Network.Execute(source);
        }

        private static void InitializeLoggingWithNlog() {
            using var loggerFactory = LoggerFactory.Create(builder => {
                builder
                    .AddFilter("Microsoft", Microsoft.Extensions.Logging.LogLevel.Warning)
                    .AddFilter("System", Microsoft.Extensions.Logging.LogLevel.Warning)
                    .SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace)
                    .AddNLog("nlog.config");
            });
            Settings.LogInstance = loggerFactory.CreateLogger("Default");
            Settings.LogThreshold = 2;
        }

        public static void RecreateDatabase(string dbName, SqlConnectionString connectionString) {
            var masterConnection = new SqlConnectionManager(connectionString.CloneWithMasterDbName());
            DropDatabaseTask.DropIfExists(masterConnection, dbName);
            CreateDatabaseTask.Create(masterConnection, dbName);
        }

        public static void Prepare() {
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
            SqlTask.ExecuteNonQuery(SqlConnection,
                "INSERT INTO source VALUES (1, 'Test - Insert')");
            SqlTask.ExecuteNonQuery(SqlConnection,
                "INSERT INTO source VALUES (2, 'Test - Update')");
            SqlTask.ExecuteNonQuery(SqlConnection,
                "INSERT INTO source VALUES (3, 'Test - Exists')");

            SqlTask.ExecuteNonQuery(SqlConnection,
                "INSERT INTO destination VALUES (2, 'XXX')");
            SqlTask.ExecuteNonQuery(SqlConnection,
                "INSERT INTO destination VALUES (3, 'Test - Exists')");
            SqlTask.ExecuteNonQuery(SqlConnection,
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
