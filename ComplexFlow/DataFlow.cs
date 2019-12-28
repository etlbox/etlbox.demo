using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace ALE.ComplexFlow {
    public class DataFlow {

        public void Run() {
            //Read data from csv file
            CSVSource sourceOrderData = new CSVSource("DemoData.csv");
            sourceOrderData.Configuration.Delimiter = ";";

            //Transform into Order object
            RowTransformation<string[], Order> transIntoObject = new RowTransformation<string[], Order>(CSVIntoObject);

            //Find corresponding customer id if customer exists in Customer table
            DBSource<Customer> sourceCustomerData = new DBSource<Customer>("demo.Customer");
            LookupCustomerKey lookupCustKeyClass = new LookupCustomerKey();
            Lookup<Order, Order, Customer> lookupCustomerKey = new Lookup<Order, Order, Customer>(
                lookupCustKeyClass.FindKey, sourceCustomerData, lookupCustKeyClass.LookupData);

            //Split data
            Multicast<Order> multiCast = new Multicast<Order>();

            //Store Order in Orders table
            DBDestination<Order> destOrderTable = new DBDestination<Order>("demo.Orders");

            //Create rating for existing customers based total of order amount
            BlockTransformation<Order,Rating> blockOrders = new BlockTransformation<Order,Rating>(BlockTransformOrders);
            DBDestination<Rating> destRating = new DBDestination<Rating>("demo.CustomerRating");

            //Link the components
            sourceOrderData.LinkTo<Order>(transIntoObject)
                           .LinkTo(lookupCustomerKey)
                           .LinkTo(multiCast)
                           .LinkTo(destOrderTable);

            multiCast.LinkTo(blockOrders);

            blockOrders.LinkTo(destRating);

            //Execute the data flow synchronous
            sourceOrderData.Execute();
            destOrderTable.Wait();
            destRating.Wait();
        }

        private Order CSVIntoObject(string[] csvLine) {
            return new Order() {
                Number = csvLine[0],
                Item = csvLine[1],
                Amount = decimal.Parse(csvLine[2].Substring(0, csvLine[2].Length - 1), CultureInfo.GetCultureInfo("en-US")),
                CustomerName = csvLine[3]
            };
        }

        public class LookupCustomerKey
        {
            public List<Customer> LookupData { get; set; } = new List<Customer>();

            public Order FindKey(Order orderRow)
            {
                var customer = LookupData.Where(cust => cust.CustomerName == orderRow.CustomerName).FirstOrDefault();
                orderRow.CustomerKey = customer?.CustomerKey ?? 0;
                return orderRow;
            }
        }

        private List<Rating> BlockTransformOrders(List<Order> allOrders) {
            List<int> allCustomerKeys = allOrders.Select(ord => ord.CustomerKey).Distinct().ToList();
            List<Rating> result = new List<Rating>();
            foreach (int custKey in allCustomerKeys) {
                Rating newRating = new Rating();
                newRating.CustomerKey = custKey;
                newRating.TotalAmount = allOrders.Where(ord => ord.CustomerKey == custKey).Sum(ord => ord.Amount);
                newRating.RatingValue = newRating.TotalAmount > 50 ? "A" : "F";
                result.Add(newRating);
            }
            return result;
        }
    }
}
