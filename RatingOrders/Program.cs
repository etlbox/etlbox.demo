using ETLBox;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using Serilog;
using Serilog.Extensions.Logging;
using System.Dynamic;
using System.Globalization;

namespace ETLBoxDemo.RatingOrdersExample {
    class Program {
        static void Main(string[] args) {
            ConnectSerilog();

            var connectionString = new SqlConnectionString(
                @"Data Source=localhost;Initial Catalog=demo;Integrated Security=false;User=sa;password=YourStrong@Passw0rd"
            );

            Settings.DefaultDbConnection = new SqlConnectionManager(connectionString);

            PrepareDb.RecreateDatabase("demo", connectionString);
            PrepareDb.Prepare();

            Settings.DefaultDbConnection = new SqlConnectionManager(connectionString);
            Run();

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        private static void ConnectSerilog() {
            var serilogLogger = new LoggerConfiguration()
                            .Enrich.FromLogContext()
                            .MinimumLevel.Information()
                            .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3} {taskName}] {Message:lj}" + Environment.NewLine)
                            .CreateLogger();

            Settings.LogInstance = new SerilogLoggerFactory(serilogLogger).CreateLogger("Default");
        }

        public static void Run() {
            Console.WriteLine("Running data flow");

            //Read data from csv file
            CsvSource sourceOrderData = new CsvSource("DemoData.csv");
            sourceOrderData.Configuration.Delimiter = ";";

            //Transform into Order object
            RowTransformation<ExpandoObject, Order> transIntoObject = new RowTransformation<ExpandoObject, Order>(
                csvLine => {
                    dynamic order = csvLine as dynamic;
                    return new Order() {
                        //Header in Csv: OrderNumber;OrderItem;OrderAmount;CustomerName
                        Number = order.OrderNumber,
                        Item = order.OrderItem,
                        Amount = decimal.Parse(order.OrderAmount.ToString().Replace("€", ""), CultureInfo.GetCultureInfo("en-US")),
                        CustomerName = order.CustomerName
                    };
                });
            sourceOrderData.LinkTo(transIntoObject);

            //Find corresponding customer id if customer exists in Customer table
            DbSource<Customer> sourceCustomerData = new DbSource<Customer>("customer");
            LookupTransformation<Order, Customer> lookupCustomerKey = new LookupTransformation<Order, Customer>(sourceCustomerData);
            transIntoObject.LinkTo(lookupCustomerKey);

            //Split data
            Multicast<Order> multiCast = new Multicast<Order>();
            lookupCustomerKey.LinkTo(multiCast);

            //Store Order in Orders table
            DbDestination<Order> destOrderTable = new DbDestination<Order>("orders");
            multiCast.LinkTo(destOrderTable);

            //Create rating for existing customers based total of order amount
            Aggregation<Order, Rating> aggregation = new Aggregation<Order, Rating>();
            multiCast.LinkTo(aggregation);

            //Store the rating in the customer rating table
            DbDestination<Rating> destRating = new DbDestination<Rating>("customer_rating");
            aggregation.LinkTo(destRating);

            //Execute the data flow synchronously
            Network.Execute(sourceOrderData);

        }
    }
}
