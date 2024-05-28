using ETLBox;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;

namespace MergePerformanceIssue {
    internal class Program {
        //Connection string for local SQL Server
        //static string connectionString = "Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=mergetest;TrustServerCertificate=true";
        //Connection string for azure SQL Server
        static string connectionString = "Server=tcp:azure.database.windows.net,1433;Initial Catalog=etlboxsupport;Persist Security Info=False;User ID=etlbox;Password=YourStrong@Passw0rd;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=True;";

        static void Main(string[] args) {

            ETLBoxOffice.LicenseManager.LicenseCheck.LicenseKey =
                @"2024-01-15|TRIAL|||Support|support@etlbox.net||Lhwz7nAQavV8oa/HHWvX2ukUehx0hIa5IkxoBGMjdPJ9p4tlnDt3iL0XHbj53gRDVqK5vST7Gi9WwM06kPZDLvYyrK8ymvdTFFX/KWkUhjpXZX8CUQ6C86BZzlwm6APLPjlFyXZjoo9OujRMtnyLI4YotO3s2ziPvkrKELhtDMk=";

            InitializeLoggingWithNlog();
            Settings.DisableAllLogging = true;
            var connMan = new SqlConnectionManager(connectionString);
            DbHelper.CleanSourceTable = true;
            DbHelper.CreateDatabaseIfNeeded("mergetest", connectionString);
            DbHelper.CreateTables(connMan);

            Console.WriteLine("Writing test data into source and destination tables ...");
            Console.WriteLine("This might take a while ...");

            //Test with small number of rows
            //DbHelper.InsertTestDataSource(connMan, 55, 107);
            //DbHelper.InsertTestDataDestination(connMan, 25, 70);
            //Test with medium number of rows
            //DbHelper.InsertTestDataSource(connMan, 550_123, 1_050_234);
            //DbHelper.InsertTestDataDestination(connMan, 250_345, 700_456);
            //Test with large number of rows
            DbHelper.InsertTestDataSource(connMan, 5_000_123, 10_000_789);
            DbHelper.InsertTestDataDestination(connMan, 2_500_345, 7_000_456);
            Settings.DisableAllLogging = false;

            DbSource<MergeRow> source = new() {
                ConnectionManager = connMan,
                TableName = "source",
                DisableLogging = true
            };
            DbMerge<MergeRow> merge = new() {
                ConnectionManager = connMan,
                TableName = "destination",
                MergeMode = MergeMode.Full,
                CacheMode = CacheMode.Full,
                DisableLogging = false
            };

            DbDestination<MergeRow> delta = new(connMan, "delta");
            delta.DisableLogging = true;
            source.LinkTo(merge);
            merge.LinkTo(delta);
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
            Settings.LogThreshold = 10000;

        }
    }
}
