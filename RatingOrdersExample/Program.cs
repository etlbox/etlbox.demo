using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using System;
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
            CsvSource<string[]> sourceOrderData = new CsvSource<string[]>("DemoData.csv");
            sourceOrderData.Configuration.Delimiter = ";";

            //Transform into Order object
            RowTransformation<string[], Order> transIntoObject = new RowTransformation<string[], Order>(
                csvLine =>
                {
                    return new Order()
                    {
                        Number = csvLine[0],
                        Item = csvLine[1],
                        Amount = decimal.Parse(csvLine[2].Substring(0, csvLine[2].Length - 1), CultureInfo.GetCultureInfo("en-US")),
                        CustomerName = csvLine[3]
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

            //Execute the data flow synchronous
            sourceOrderData.Execute();
            destOrderTable.Wait();
            destRating.Wait();
        }
    }
}
