using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using Serilog;
using Serilog.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;

namespace ETLBoxDemo.SimpeFlow {
    class Program {
        static void Preparation() {
            SqlConnectionManager dbConnection = new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true");

            //Create destination table
            DropTableTask.DropIfExists(dbConnection, "OrderTable");
            CreateTableTask.Create(dbConnection, "OrderTable", new List<TableColumn>()
            {
                new TableColumn("Id", "INT", allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Item", "NVARCHAR(50)"),
                new TableColumn("Quantity", "INT"),
                new TableColumn("Price", "DECIMAL(10,2)")
            });
        }

        static async Task Main(string[] args) {
            //Set up database and logging
            ConnectSerilog();
            Preparation();

            await DataFlow_POCO();

            await DataFlow_Dynamic();

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        private static async Task DataFlow_POCO() {
            //Define the source
            CsvSource<MyRow> source = new CsvSource<MyRow>("demodata.csv");

            //Define the transformation
            RowTransformation<MyRow, Order> rowTrans = new RowTransformation<MyRow, Order>();
            rowTrans.TransformationFunc =
              row => new Order() {
                  Item = row.name,
                  Quantity = int.Parse(row.quantity_m) + int.Parse(row.quantity_l),
                  Price = int.Parse(row.price_in_cents) / 100
              };

            //Define the destination            
            SqlConnectionManager connMan = new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;");
            DbDestination<Order> dest = new DbDestination<Order>(connMan, "OrderTable");

            //Link & run flow
            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            await Network.ExecuteAsync(source);
        }

        private static async Task DataFlow_Dynamic() {
            //Define the source
            CsvSource source = new CsvSource("demodata.csv");

            //Define the transformation
            RowTransformation<ExpandoObject, Order> rowTrans = new RowTransformation<ExpandoObject, Order>();
            rowTrans.TransformationFunc =
              row => {
                  dynamic dynamicRow = row as ExpandoObject;
                  IDictionary<string, object> dictRow = row as IDictionary<string, object>;
                  Order order = new Order() {
                      Item = dynamicRow.name,
                      Quantity = int.Parse(dictRow["quantity_m"].ToString()) + int.Parse(dictRow["quantity_l"].ToString()),
                      Price = int.Parse(dictRow["price_in_cents"].ToString()) / 100
                  };
                  return order;
              };

            //Define the destination            
            SqlConnectionManager connMan = new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;");
            DbDestination<Order> dest = new DbDestination<Order>(connMan, "OrderTable");

            //Link & run flow
            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            await Network.ExecuteAsync(source);
        }

        static void ConnectSerilog() {
            var serilogLogger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .MinimumLevel.Information()
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3} {taskName}] {Message:lj}" + Environment.NewLine)
                .CreateLogger();

            Settings.LogInstance = new SerilogLoggerFactory(serilogLogger).CreateLogger("Default");
        }
    }
}
