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
    public class Order
    {
        public int Id { get; set; }
        public string Number { get; set; }
        public string Details { get; set; }
        public DateTime Date { get; set; }
    }

    public class SyncData
    {
        [AggregateColumn("Id", AggregationMethod.Max)]
        public int SyncId { get; set; }
    }

    public static class DataLakeDemo
    {
        static string containerName;

        [FunctionName("DataLake")]
        public static async Task Run([TimerTrigger("0 */1 * * * *"
            //, RunOnStartup=true) //only for testing purposes
            )]TimerInfo myTimer, ILogger log) {

            Logging.LogInstance = log;            

            string sqlConnectionString = Environment.GetEnvironmentVariable("SqlServerConnectionString", EnvironmentVariableTarget.Process);
            string storageConnString = Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);

            var conn = new SqlConnectionManager(sqlConnectionString);

 
            if (!DemoHelper.WasInitialized)
                containerName = DemoHelper.PrepareForDemo(storageConnString, conn);

            SyncData syncDataLastRun = ReadLastSyncKey();

            var dbSource = new DbSource<Order>() {
                ConnectionManager = conn,
                Sql = $"SELECT Id, Number, Details, Date FROM Orders WHERE Id > {syncDataLastRun.SyncId} ORDER BY Date"
            };

            var jsonDest = new JsonDestination<Order>();
            jsonDest.ResourceType = ResourceType.AzureBlob;
            jsonDest.AzureBlobStorage.ConnectionString = storageConnString;
            jsonDest.AzureBlobStorage.ContainerName = containerName;

            var currentDate = new DateTime(1900, 1, 1);
            jsonDest.HasNextUri = (_, order) => {
                if (order.Date.Date > currentDate.Date) {
                    currentDate = order.Date;
                    return true;
                }
                return false;
            };
            jsonDest.GetNextUri = (_, order) => "OrderData_" + order.Date.ToString("yyyy-MM-dd") + ".json";

            var multicast = new Multicast<Order>();
            var aggregation = new Aggregation<Order, SyncData>();
            var syncMemoryDest = new MemoryDestination<SyncData>();

            /*
             *                  |---> jsonDest ("OrderData_2020-01-01.json", "OrderData_2020-01-02.json", ..)
             *                  |
             *  dbSource --> multicast
             *                  |
             *                  |---> aggregation --> syncMemoryDest (1st run: SyncId = 5, 2nd run: SyncId = 7)
             */
            dbSource.LinkTo(multicast);
            multicast.LinkTo(jsonDest);
            multicast.LinkTo(aggregation);
            aggregation.LinkTo(syncMemoryDest);

            Network.Execute(dbSource);

            if (syncMemoryDest.Data.Count > 0) {
                SyncData syncDataThisRun = syncMemoryDest.Data.First();
                StoreLastSyncKey(syncDataThisRun);
            }
            
        }

        private static SyncData ReadLastSyncKey() {
            try {
                var syncsource = new JsonSource<SyncData>("LastSyncId.json");
                syncsource.DisableLogging = true;
                var memdest = new MemoryDestination<SyncData>();
                memdest.DisableLogging = true;
                syncsource.LinkTo(memdest);
                Network.Execute(syncsource);
                return memdest.Data.First();
            } catch {

            }
            return new SyncData() {
                SyncId = -1
            };
        }

        private static void StoreLastSyncKey(SyncData syncData) {
            var memsource = new MemorySource<SyncData>();
            memsource.DisableLogging = true;
            memsource.DataAsList.Add(syncData);
            var syncdest = new JsonDestination<SyncData>("LastSyncId.json");
            syncdest.DisableLogging = true;
            memsource.LinkTo(syncdest);
            Network.Execute(memsource);
        }
    }
}
