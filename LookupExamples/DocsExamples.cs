using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LookupExamples
{
    internal class DocsExamples
    {
        public SqlConnectionManager SqlConnection => new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;");


        public void Prepare()
        {
            RecreateDatabase("demo", SqlConnection.ConnectionString as SqlConnectionString);
            DropTableTask.DropIfExists(SqlConnection, "CustomerTable");
            var td = new TableDefinition("CustomerTable"
                , new List<TableColumn>() {
                new TableColumn("Id", "INT", allowNulls: false),
                new TableColumn("Name", "NVARCHAR(100)", allowNulls: true)
            });
            td.CreateTable(SqlConnection);

            SqlTask.ExecuteNonQuery(SqlConnection,
                "INSERT INTO CustomerTable VALUES (1,'John')");
            SqlTask.ExecuteNonQuery(SqlConnection,         
                "INSERT INTO CustomerTable VALUES (2,'Jim')");
        }

        void RecreateDatabase(string dbName, SqlConnectionString connectionString)
        {
            var masterConnection = new SqlConnectionManager(connectionString.CloneWithMasterDbName());
            DropDatabaseTask.DropIfExists(masterConnection, dbName);
            CreateDatabaseTask.Create(masterConnection, dbName);
        }


        public class Order
        {
            public int OrderNumber { get; set; }
            public string CustomerName { get; set; }
            public int? CustomerId { get; set; }
        }

        public class Customer
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public void UsingRowTransformation()
        {
            Prepare();
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });

            var rowTrans = new RowTransformation<Order>(
               row => {
                   int? id = SqlTask.ExecuteScalar<int>(SqlConnection,
                     sql: $"SELECT Id FROM CustomerTable WHERE Name='{row.CustomerName}'");
                   row.CustomerId = id;
                   return row;
               });


            /* Delete below here */

            var dest = new MemoryDestination<Order>();

            orderSource.LinkTo(rowTrans).LinkTo(dest);
            orderSource.Execute();
        }

        public void UsingLookup()
        {
            Prepare();
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });

            var lookupSource = new DbSource<Customer>(SqlConnection, "CustomerTable");

            var lookup = new LookupTransformation<Order, Customer>();
            lookup.Source = lookupSource;
            lookup.RetrievalFunc =
                (row, cache) => {
                    row.CustomerId = cache.Where(cust => cust.Name == row.CustomerName)
                                          .Select(cust => cust.Id)
                                          .FirstOrDefault();
                    return row;
                };

            var dest = new MemoryDestination<Order>();

            orderSource.LinkTo(lookup).LinkTo(dest);
            Network.Execute(orderSource);

            foreach (var row in dest.Data)
                Console.WriteLine($"Order:{row.OrderNumber} Name:{row.CustomerName} Id:{row.CustomerId}");

            //Output
            //Order:815 Name:John Id:1 
            //Order:4711 Name:Jim Id:2
        }

        public class CustomerWithAttr
        {
            [RetrieveColumn(nameof(Order.CustomerId))]
            public int Id { get; set; }
            [MatchColumn(nameof(Order.CustomerName))]
            public string Name { get; set; }
        }

        public void UsingLookupWithAttributes()
        {
            Prepare();
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });

            var lookupSource = new DbSource<CustomerWithAttr>(SqlConnection, "CustomerTable");

            var lookup = new LookupTransformation<Order, CustomerWithAttr>();
            lookup.Source = lookupSource;

            var dest = new MemoryDestination<Order>();

            orderSource.LinkTo(lookup).LinkTo(dest);
            Network.Execute(orderSource);

            foreach (var row in dest.Data)
                Console.WriteLine($"Order:{row.OrderNumber} Name:{row.CustomerName} Id:{row.CustomerId}");

            //Output
            //Order:815 Name:John Id:1 
            //Order:4711 Name:Jim Id:2
        }

        public void AttributesWithDynamic()
        {
            Prepare();
            var orderSource = new MemorySource();
            dynamic sourceRow1 = new ExpandoObject();
            sourceRow1.OrderNumber = 815;
            sourceRow1.CustomerName = "John";
            orderSource.DataAsList.Add(sourceRow1);
            dynamic sourceRow2 = new ExpandoObject();
            sourceRow2.OrderNumber = 4711;
            sourceRow2.CustomerName = "Jim";
            orderSource.DataAsList.Add(sourceRow2);

            var lookupSource = new DbSource(SqlConnection, "CustomerTable");

            var lookup = new LookupTransformation();
            lookup.MatchColumns = new[] {
                new MatchColumn() { LookupSourcePropertyName = "Name"
                , InputPropertyName = "CustomerName"
                }
            };
            lookup.RetrieveColumns = new[] {
                new RetrieveColumn() {
                    LookupSourcePropertyName = "Id",
                    InputPropertyName = "CustomerId"
                }
            };
            lookup.Source = lookupSource;

            var dest = new MemoryDestination();

            orderSource.LinkTo(lookup).LinkTo(dest);
            Network.Execute(orderSource);

            foreach (dynamic row in dest.Data)
                Console.WriteLine($"Order:{row.OrderNumber} Name:{row.CustomerName} Id:{row.CustomerId}");

            //Output
            //Order:815 Name:John Id:1 
            //Order:4711 Name:Jim Id:2
        }

        public void UsingLookupWithRetrievalByKeyFunc()
        {
            Prepare();
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });

            var lookupSource = new DbSource<Customer>(SqlConnection, "CustomerTable");

            var lookup = new LookupTransformation<Order, Customer>();
            lookup.Source = lookupSource;
            lookup.GetInputRecordKeyFunc = inputrow => inputrow.CustomerName;
            lookup.GetSourceRecordKeyFunc = sourcerow => sourcerow.Name;
            lookup.RetrievalByKeyFunc = (inputrow, dict) => {
                if (dict.ContainsKey(inputrow.CustomerName))
                    inputrow.CustomerId = dict[inputrow.CustomerName].Id;
                return inputrow;
            };

            var dest = new MemoryDestination<Order>();

            orderSource.LinkTo(lookup).LinkTo(dest);
            Network.Execute(orderSource);

            foreach (var row in dest.Data)
                Console.WriteLine($"Order:{row.OrderNumber} Name:{row.CustomerName} Id:{row.CustomerId}");

            //Output
            //Order:815 Name:John Id:1 
            //Order:4711 Name:Jim Id:2
        }

        public void PartialDbCacheWithAttributes()
        {
            Prepare();
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });

            var lookupSource = new DbSource<CustomerWithAttr>(SqlConnection, "CustomerTable");

            var lookup = new LookupTransformation<Order, CustomerWithAttr>();
            lookup.Source = lookupSource;
            lookup.CacheMode = CacheMode.Partial;
            lookup.PartialCacheSettings.LoadBatchSize = 1;

            var dest = new MemoryDestination<Order>();

            orderSource.LinkTo(lookup).LinkTo(dest);
            Network.Execute(orderSource);

            foreach (var row in dest.Data)
                Console.WriteLine($"Order:{row.OrderNumber} Name:{row.CustomerName} Id:{row.CustomerId}");

            //Output
            //Order:815 Name:John Id:1 
            //Order:4711 Name:Jim Id:2


            /* Delete below here */
        }

        public void PartialDbCacheWithSql()
        {
            Prepare();
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });

            var lookupSource = new DbSource<CustomerWithAttr>(SqlConnection, "CustomerTable");

            var lookup = new LookupTransformation<Order, CustomerWithAttr>();
            lookup.Source = lookupSource;
            lookup.CacheMode = CacheMode.Partial;
            lookup.PartialCacheSettings.LoadBatchSize = 1;
            lookup.PartialCacheSettings.LoadCacheSql = batch =>
                $@"SELECT Id, Name
                    FROM CustomerTable
                    WHERE Name in ({string.Join(",", batch.Select(r => $"'{r.CustomerName}'"))})";

            var dest = new MemoryDestination<Order>();

            orderSource.LinkTo(lookup).LinkTo(dest);
            Network.Execute(orderSource);

            foreach (var row in dest.Data)
                Console.WriteLine($"Order:{row.OrderNumber} Name:{row.CustomerName} Id:{row.CustomerId}");

            //Output
            //Order:815 Name:John Id:1 
            //Order:4711 Name:Jim Id:2
        }
    }
}
