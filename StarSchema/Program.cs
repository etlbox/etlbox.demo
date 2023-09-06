using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.ComponentModel;
using System.Security.Principal;

namespace ETLBoxDemo.StarSchema
{
    internal class Program
    {
        static string OLTP_ConnectionString = @"Data Source=localhost;Initial Catalog=OLTP_DB;Integrated Security=false;User=sa;password=YourStrong@Passw0rd";
        static string OLAP_ConnectionString = @"Data Source=localhost;Initial Catalog=OLAP_DB;Integrated Security=false;User=sa;password=YourStrong@Passw0rd";
        static SqlConnectionManager OLTP_Connection => new SqlConnectionManager(OLTP_ConnectionString);
        static SqlConnectionManager OLAP_Connection => new SqlConnectionManager(OLAP_ConnectionString);
        static void Main(string[] args) {
            PrepareDb.RecreateDatabase("OLTP_DB", OLTP_ConnectionString);
            PrepareDb.RecreateDatabase("OLAP_DB", OLTP_ConnectionString);
            PrepareDb.CreateOLTPTables(OLTP_ConnectionString);
            PrepareDb.CreateStarSchema(OLAP_ConnectionString);

            LoadCustomer_SCD1();
            ChangeCustomer();
            LoadCustomer_SCD1();

            LoadProducts_SCD2(new DateTime(2023, 1, 1));
            ChangeProduct();
            LoadProducts_SCD2(new DateTime(2023, 1, 4));
            ChangeProduct2();
            LoadProducts_SCD2(new DateTime(2023, 1, 5));
        }

        private static void ChangeProduct() {
            SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Product VALUES('P-00014','Eletric Toothbrush', 'Using AI for your teeth',99, '2023-01-04')");
            SqlTask.ExecuteNonQuery(OLTP_Connection, "UPDATE Product SET Description = 'Best notebook on the market', LastUpdated = '2023-01-04' WHERE ProductNumber = 'P-00013'");
            SqlTask.ExecuteNonQuery(OLTP_Connection, "UPDATE Product SET RecommendedPrice = 499, LastUpdated = '2023-01-04' WHERE ProductNumber = 'P-00010'");
        }
        private static void ChangeProduct2() {
            SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Product VALUES('P-00015','Remote control', NULL ,99, '2023-01-05')");
            SqlTask.ExecuteNonQuery(OLTP_Connection, "UPDATE Product SET RecommendedPrice = 599, LastUpdated = '2023-01-05' WHERE ProductNumber = 'P-00010'");
        }

        public class Product
        {
            [RetrieveColumn("DimId")]
            public int? DimId { get; set; }
            public string Name { get; set; }
            [ColumnMap("ProductNumber")]
            [MatchColumn("Number")]
            public string Number { get; set; }
            public string Description { get; set; }
            public decimal RecommendedPrice { get; set; }
            public DateTime LastUpdated { get; set; }
            [RetrieveColumn("ValidFrom")]
            public DateTime? ValidFrom { get; set; } = new DateTime(1900, 1, 1);
            public DateTime? ValidTo { get; set; } = new DateTime(9999, 12, 31);
        }
        private static void LoadProducts_SCD2(DateTime lastUpdateDate) {
            var source = new DbSource<Product>(OLTP_Connection);
            source.Sql = "SELECT ProductNumber, Name, Description, RecommendedPrice, LastUpdated FROM Product WHERE LastUpdated >= @par1";
            source.SqlParameter = new[] { 
                new QueryParameter() {
                    Name="par1", Type = "DATETIME", Value = lastUpdateDate
                }
            };

            var lookup = new LookupTransformation<Product, Product>();
            var lookupSource = new DbSource<Product>(OLAP_Connection, "DimProduct");
            lookupSource.Sql = @"
SELECT DISTINCT ProductNumber,
     LAST_VALUE(ValidFrom) OVER(
         PARTITION BY ProductNumber ORDER BY ValidFrom
         RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING
    ) ValidFrom,
    LAST_VALUE(DimId) OVER(
         PARTITION BY ProductNumber ORDER BY DimId
         RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING
    ) DimId
FROM DimProduct";
            lookup.Source = lookupSource;

            var destNewRows = new DbDestination<Product>(OLAP_Connection, "DimProduct");
            var destChangedRows = new DbDestination<Product>(OLAP_Connection, "DimProduct");
            var destUpdateRows = new DbDestination<Product>(OLAP_Connection, "DimProduct");

            destUpdateRows.BulkOperation = BulkOperation.Update;
            destUpdateRows.IdColumns = new[] {
                new IdColumn() { IdPropertyName = "Number"},
                new IdColumn() { IdPropertyName = "ValidFrom"},
            };
            destUpdateRows.UpdateColumns = new[] {
                new UpdateColumn() { UpdatePropertyName ="ValidTo"}
            };

            var multicast = new Multicast<Product>();
           
            var updateValidFromTransformation = new RowTransformation<Product>(
                row => {
                    row.ValidFrom = row.LastUpdated;
                    return row;
                });

            var updateValidToTransformation = new RowTransformation<Product>(
               row => {
                   row.ValidTo = row.LastUpdated.AddSeconds(-1);
                   return row;
               });

            source.LinkTo(lookup);
            lookup.LinkTo(destNewRows, row => row.DimId == null);
            lookup.LinkTo(multicast, row => row.DimId != null);
            multicast.LinkTo(updateValidFromTransformation).LinkTo(destChangedRows);
            multicast.LinkTo(updateValidToTransformation).LinkTo(destUpdateRows);
            Network.Execute(source);

        }

        public class Customer : MergeableRow
        {
            public string Name { get; set; }
            [IdColumn]
            [ColumnMap("CustomerNumber")]
            public string Number { get; set; }
        }

        private static void LoadCustomer_SCD1() {
            var source = new DbSource<Customer>(OLTP_Connection, "Customer");

            var dest = new DbMerge<Customer>(OLAP_Connection, "DimCustomer");
            dest.MergeMode = MergeMode.Full;

            source.LinkTo(dest);
            Network.Execute(source);
        }

        private static void ChangeCustomer() {
            SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Customer VALUES('C-1003','Nick Newman')");
            SqlTask.ExecuteNonQuery(OLTP_Connection, "UPDATE Customer SET Name='The Batman' WHERE CustomerNumber = 'C-1002'");
        }
    }
}