using CsvHelper.Configuration.Attributes;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LookupExamples
{
    internal class AlternativeExamples
    {
        public SqlConnectionManager SqlConnection => new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;");

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

        public void UsingBatchTransformation()
        {
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });

            var batchTrans = new BatchTransformation<Order>()
            {
                BatchSize = 100,
                BatchTransformationFunc =
                batch =>
                {
                    var names = string.Join(",", batch.Select(cust => $"'{cust.CustomerName}'"));
                    string curName =""; int curId =0;
                    Dictionary<string, int> idByName = new Dictionary<string, int>();
                    var sql = new SqlTask()
                    {
                        ConnectionManager = SqlConnection,
                        Sql = $"SELECT DISTINCT Name, Id FROM CustomerTable WHERE Name IN ({names})",
                        AfterRowReadAction = () => { idByName.Add(curName, curId); },
                        Actions = new List<Action<object>>() {
                            name => curName = (string)name,
                            id => curId = (int)id
                        }
                    };
                    sql.ExecuteReader();
                    foreach (var row in batch)
                        row.CustomerId = idByName[row.CustomerName];
                    return batch;
                }
            };
            var dest = new MemoryDestination<Order>();
            orderSource.LinkTo(batchTrans).LinkTo(dest);
            Network.Execute(orderSource);

            foreach (var result in dest.Data)
                Console.WriteLine($"Customer {result.CustomerName} has id {result.CustomerId}");
        }

        internal void UsingMergeJoin()
        {
            var orderSource = new MemorySource<Order>();
            orderSource.DataAsList.Add(new Order() { OrderNumber = 815, CustomerName = "John" });
            orderSource.DataAsList.Add(new Order() { OrderNumber = 4711, CustomerName = "Jim" });
            var customerSource = new MemorySource<Customer>();
            customerSource.DataAsList.Add(new Customer() { Id = 1, Name = "John" });
            customerSource.DataAsList.Add(new Customer() { Id = 2, Name = "Jim"});

            var join = new MergeJoin<Order, Customer, Order>(
                (leftRow, rightRow) =>
                {
                    if (rightRow == null || leftRow == null) //NoMatch
                return null;
                    else
                        return new Order() { CustomerId = rightRow.Id, CustomerName = leftRow.CustomerName, OrderNumber = leftRow.OrderNumber };

                });

            join.ComparisonFunc = (leftRow, rightRow) => string.Compare(leftRow.CustomerName, rightRow.Name);

            var dest = new MemoryDestination<Order>();
            orderSource.LinkTo(join.LeftInput);
            customerSource.LinkTo(join.RightInput);
            join.LinkTo(dest);

            Network.Execute(orderSource, customerSource);

            foreach (var result in dest.Data)
                Console.WriteLine($"Customer {result.CustomerName} has id {result.CustomerId}");
        }        
    }
}
