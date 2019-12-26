using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace ALE.ComplexFlow {
    public class DataFlowTasks {

        public void Run() {
            //Read data from csv file
            CSVSource sourceOrderData = new CSVSource("DemoData.csv");
            sourceOrderData.Configuration.Delimiter = ";";

            //Transfrom into Order object
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
            BlockTransformation<Order> blockOrders = new BlockTransformation<Order>(BlockTransformOrders);
            DBDestination<Rating> destRating = new DBDestination<Rating>("demo.CustomerRating");
            RowTransformation<Order, Rating> transOrderIntoCust = new RowTransformation<Order, Rating>(OrderIntoRating);

            //Link the components
            sourceOrderData.LinkTo<Order>(transIntoObject)
                           .LinkTo(lookupCustomerKey)
                           .LinkTo(multiCast)
                           .LinkTo(destOrderTable);

            multiCast.LinkTo(blockOrders);

            blockOrders.LinkTo(transOrderIntoCust, ord => ord.Rating != null, ord => ord.Rating == null);
            transOrderIntoCust.LinkTo(destRating);

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

        private List<Order> BlockTransformOrders(List<Order> allOrders) {
            List<int> allCustomerKeys = allOrders.Select(ord => ord.CustomerKey).Distinct().ToList();
            foreach (int custKey in allCustomerKeys) {
                var firstOrder = allOrders.Where(ord => ord.CustomerKey == custKey).FirstOrDefault();
                firstOrder.Rating = new Rating();
                firstOrder.Rating.CustomerKey = custKey;
                firstOrder.Rating.TotalAmount = allOrders.Where(ord => ord.CustomerKey == custKey).Sum(ord => ord.Amount);
                firstOrder.Rating.RatingValue = firstOrder.Rating.TotalAmount > 50 ? "A" : "F";
            }
            return allOrders;
        }

        private Rating OrderIntoRating(Order orderRow) {
            return new Rating() {
                CustomerKey = orderRow.CustomerKey,
                TotalAmount = orderRow.Rating.TotalAmount,
                RatingValue = orderRow.Rating.RatingValue
            };
        }
    }
}
