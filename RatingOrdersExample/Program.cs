using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using System;
using System.Dynamic;
using System.Globalization;

namespace ALE.ComplexFlow {
    class Program {
        static void Main(string[] args) {

            var connectionString = new SqlConnectionString(
                @"Data Source=10.211.55.2;Initial Catalog=demo;Integrated Security=false;User=sa;password=YourStrong@Passw0rd");

            ControlFlow.DefaultDbConnection = new SqlConnectionManager(connectionString);

            PrepareDb.RecreateDatabase("demo", connectionString);
            PrepareDb.Prepare();

            ControlFlow.DefaultDbConnection = new SqlConnectionManager(connectionString);
            Run();

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        public static void Run()
        {
            Console.WriteLine("Running data flow");

            //Read data from csv file
            CsvSource sourceOrderData = new CsvSource("DemoData.csv");
            sourceOrderData.Configuration.Delimiter = ";";

            //Transform into Order object
            RowTransformation<ExpandoObject, Order> transIntoObject = new RowTransformation<ExpandoObject, Order>(
                csvLine =>
                {
                    dynamic order = csvLine as dynamic;
                    return new Order()
                    {
                        //Header in Csv: OrderNumber;OrderItem;OrderAmount;CustomerName
                        Number = order.OrderNumber,
                        Item = order.OrderItem,
                        Amount = decimal.Parse(order.OrderAmount.ToString().Replace("€",""), CultureInfo.GetCultureInfo("en-US")),
                        CustomerName =  order.CustomerName
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
            sourceOrderData.Execute();
            destOrderTable.Wait();
            destRating.Wait();
        }
    }
}
