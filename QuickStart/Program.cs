using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Json;
using ETLBox.SqlServer;
using Serilog;
using Serilog.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;

namespace QuickStart {
    public class OrderRow {
        [DbColumnMap("Id")]
        public long OrderNumber { get; set; }
        public int CustomerId { get; set; }
        public string Description { get; set; }
        public int Quantity { get; set; }
        public string CustomerName { get; set; }
    }

    //public class Customer
    //{
    //    [MatchColumn("CustomerId")]
    //    public int Id { get; set; }
    //    [RetrieveColumn("CustomerName")]
    //    public string Name { get; set; }
    //}

    class Program {
        static SqlConnectionManager sqlConnMan =
            new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true");

        /* Data flow
         *          
         * JsonSource --> RowTransformation --> Lookup --> Multicast --> DbDestination ("orders" table)
         * (Order data)                        |             |
         *                   CsvSource     <----             --------> TextDestination ("order_data.log")       
         *                  ("customer.csv")
         */


        async static Task Main(string[] args) {
            //Preparation
            RecreateTargetTable();
            ConnectSerilog();

            //Step 1 - creating the components
            var source = new JsonSource<OrderRow>("https://www.etlbox.net/demo/api/orders", ResourceType.Http);

            var rowTransformation = new RowTransformation<OrderRow>();
            rowTransformation.TransformationFunc = row => {
                row.Quantity = int.Parse(row.Description.Split(":").ElementAt(1));
                return row;
            };

            var lookup = new LookupTransformation<OrderRow, ExpandoObject>();
            lookup.Source = new CsvSource("files/customer.csv");

            lookup.MatchColumns = new[] {
                new MatchColumn() { LookupSourcePropertyName = "Id", InputPropertyName = "CustomerId" }
            };
            lookup.RetrieveColumns = new[] {
                new RetrieveColumn() { LookupSourcePropertyName = "Name", InputPropertyName = "CustomerName" }
            };

            var multicast = new Multicast<OrderRow>();

            var dbDest = new DbDestination<OrderRow>(sqlConnMan, "orders");
            var textDest = new TextDestination<OrderRow>("files/order_data.log");
            textDest.WriteLineFunc = row => {
                return $"{row.OrderNumber}\t{row.CustomerName}\t{row.Quantity}";
            };

            //Step2 - linking components
            source.LinkTo(rowTransformation);
            rowTransformation.LinkTo(lookup);
            lookup.LinkTo(multicast);
            multicast.LinkTo(dbDest);
            multicast.LinkTo(textDest, 
                row => row.CustomerName == "Clark Kent", 
                row => row.CustomerName != "Clark Kent");

            //Step3 - executing the network
            await Network.ExecuteAsync(source);  
            //Alternative: Network.Execute(source), shortcut for Network.ExecuteAsync(source).Wait();
        }

        static void ConnectSerilog() {
            var serilogLogger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .MinimumLevel.Debug()
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3} {taskName}] {Message:lj}" + Environment.NewLine)
                .CreateLogger();

            Settings.LogInstance = new SerilogLoggerFactory(serilogLogger).CreateLogger("Default");
        }

        static void RecreateTargetTable() {
            DropTableTask.DropIfExists(sqlConnMan, "orders");
            CreateTableTask.Create(sqlConnMan, "orders", new List<TableColumn>()
            {
                new TableColumn("Id", "INT", allowNulls:false, isPrimaryKey:true),
                new TableColumn("Description", "VARCHAR(50)"),
                new TableColumn("CustomerName", "VARCHAR(500)"),
                new TableColumn("Quantity", "SMALLINT")
            });
        }
    }
}

