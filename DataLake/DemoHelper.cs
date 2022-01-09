using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBox.Logging;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace DataLakeDemo
{
    public static class DemoHelper
    {

        public static bool WasInitialized = false;

        internal static string PrepareForDemo(string storageConnString, SqlConnectionManager conn) {
            PrepareDatabaseTable(conn);
            RunTestDataCreationTask(conn);
            var containerName = "datalake" + Guid.NewGuid().ToString();
            CreateContainer(storageConnString, containerName);
            DeleteLastSyncKey();
            WasInitialized = true;            
            return containerName;
        }

         
      

        private static void PrepareDatabaseTable(IConnectionManager connection) {
            TableDefinition td = new TableDefinition("orders");
            td.Columns = new List<TableColumn>() {
                new TableColumn("Id","INT",isIdentity:true, isPrimaryKey:true, allowNulls:false),
                new TableColumn("Number","VARCHAR(10)",allowNulls:false),
                new TableColumn("Details","VARCHAR(200)",allowNulls:true),
                new TableColumn("Date","DATETIME",allowNulls:false)
            };
            DropTableTask.DropIfExists(connection, "orders");
            CreateTableTask.Create(connection, td);
        }

        private static void RunTestDataCreationTask(IConnectionManager connection) {
            int startNumber = 1;
            int days = 1;
            Random rnd = new Random();
            var t = Task.Run(() => {
                while (true) {
                    var pastDate = DateTime.Now.AddDays(days);                   
                    QueryParameter par1 = new QueryParameter() { Name = "number", Value = "OD"+startNumber };
                    QueryParameter par2 = new QueryParameter() { Name = "details", Value = GetDetailsText(startNumber) };
                    QueryParameter par3 = new QueryParameter() { Name = "date", Value = pastDate };

                    var sql = new SqlTask(
                        "INSERT INTO orders (Number, Details, Date) VALUES (@number, @details, @date)",
                        new[] { par1, par2, par3 }
                        ) {
                        ConnectionManager = connection,
                        DisableLogging = true
                    }
                    .ExecuteNonQuery();
                    if (startNumber % 8 == 0) days++;
                    startNumber++;
                    Task.Delay(rnd.Next(0,4)*1000).Wait();
                }
            });
        }

        private static object GetDetailsText(int startNumber) {
            List<string> details = new List<string>() { "T-Shirt", "Jeans", "Socks", "Clock", "Bag" };
            return details.ElementAt(startNumber % 5);
        }

        private static void DeleteLastSyncKey() {
            if (File.Exists("LastSyncId.json"))
                File.Delete("LastSyncId.json");
        }

        private static void CreateContainer(string storageConnString, string containerName) {
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnString);            
            blobServiceClient.CreateBlobContainerAsync(containerName).Wait();
        }




        
    }
}
