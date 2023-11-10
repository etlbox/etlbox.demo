using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Json;
using ETLBox.SqlServer;
using System;
using System.Linq;

namespace ETLBoxDemo.StarSchema;

internal class Program
{
    static string OLTP_ConnectionString = @"Data Source=localhost;Initial Catalog=OLTP_DB;Integrated Security=false;User=sa;password=YourStrong@Passw0rd;TrustServerCertificate=true";
    static string OLAP_ConnectionString = @"Data Source=localhost;Initial Catalog=OLAP_DB;Integrated Security=false;User=sa;password=YourStrong@Passw0rd;TrustServerCertificate=true";
    static SqlConnectionManager OLTP_Connection => new SqlConnectionManager(OLTP_ConnectionString);
    static SqlConnectionManager OLAP_Connection => new SqlConnectionManager(OLAP_ConnectionString);

    static void Main(string[] args) {
        Console.WriteLine("(Re)Creating demo databases 'OLTP_DB' & 'OLAP_DB'");
        PrepareDb.RecreateDatabase("OLTP_DB", OLTP_ConnectionString);
        PrepareDb.RecreateDatabase("OLAP_DB", OLTP_ConnectionString);

        Console.WriteLine("Create OLTP tables");
        PrepareDb.CreateOLTPTables(OLTP_ConnectionString);
        Console.WriteLine("Create OLAP tables");
        PrepareDb.CreateStarSchema(OLAP_ConnectionString);

        Console.WriteLine("Load customer as Slowy Changing Dimension Type 1");
        LoadCustomer_SCD1();
        Console.WriteLine("Changing customer data & reloading");
        ChangeCustomer();
        LoadCustomer_SCD1();
        Console.WriteLine("Customer dimension successfully loaded!");

        Console.WriteLine("Load products as Slowy Changing Dimension Type 2");
        LoadProducts_SCD2(new DateTime(2023, 1, 1));
        Console.WriteLine("Changing product data & reloading");
        ChangeProduct();
        LoadProducts_SCD2(new DateTime(2023, 1, 4));
        Console.WriteLine("Changing product data again & reloading");
        ChangeProduct2();
        LoadProducts_SCD2(new DateTime(2023, 1, 5));
        Console.WriteLine("Product dimension successfully loaded!");

        Console.WriteLine("Creating a generic date dimension");
        CreateDateDimension();

        Console.WriteLine("Loading Fact data - starting with data since beginning");
        DateTime lastLoadDate = new DateTime(1980, 1, 1);
        LoadOrders(lastLoadDate);

        Console.WriteLine("Adding fact data and loading delta");
        lastLoadDate = GetMaxOrderDate();
        AddOrders();
        LoadOrders(lastLoadDate);

        Console.WriteLine("Adding fact data with one flawed record and loading again - flawed entry is redirected into 'error.json'");
        lastLoadDate = GetMaxOrderDate();
        AddOrderWithFlawedRecord();
        LoadOrders(lastLoadDate);
        Console.WriteLine("Fact table successfully loaded!");
    }


    private static void CreateDateDimension() {
        DropTableTask.DropIfExists(OLAP_Connection, "DimDate");
        CreateTableTask.CreateIfNotExists(OLAP_Connection, "DimDate",
            typeof(DateDimension).GetProperties()
            .Where(prop => prop.Name != nameof(DateDimension.Date))
            .Select(prop => 
                new TableColumn(
                name: prop.Name,
                dataType: prop.PropertyType == typeof(int) ? "INT" : "VARCHAR(30)",
                allowNulls: false,
                isPrimaryKey: prop.Name == nameof(DateDimension.DateID)
                )).ToList());

        var source = new MemorySource<DateDimension>();
        source.Data = DateDimension.Generate(new DateTime(2023, 1, 1), new DateTime(2023, 1, 5));
        var dest = new DbDestination<DateDimension>(OLAP_Connection, "DimDate");
        source.LinkTo(dest);
        Network.Execute(source);
    }



    #region Customer

    public class Customer : MergeableRow
    {            
        public string Name { get; set; }
        [IdColumn]
        [DbColumnMap("CustomerNumber")]
        public string Number { get; set; }
        public int? DimId { get; set; }
    }

    private static void LoadCustomer_SCD1() {
        var source = new DbSource<Customer>(OLTP_Connection, "Customer");

        var dest = new DbMerge<Customer>(OLAP_Connection, "DimCustomer");
        dest.MergeMode = MergeMode.InsertsAndUpdates;

        source.LinkTo(dest);
        Network.Execute(source);
    }

    private static void ChangeCustomer() {
        SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Customer VALUES('C-1003','Kevin Justin')");
        SqlTask.ExecuteNonQuery(OLTP_Connection, "UPDATE Customer SET Name='Jack Apples' WHERE CustomerNumber = 'C-1002'");
    }

    #endregion

    #region Product

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
        [DbColumnMap("ProductNumber")]
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
                Name="par1", SqlType = "DATETIME", Value = lastUpdateDate
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

    #endregion

    #region Orders

    public class Order
    {
        [DbColumnMap("SourceOrderId")]
        public int OrderId { get; set; }
        public DateTime OrderDate { get; set; }
        public int? DateDim => int.Parse(OrderDate.ToString("yyyyMMdd"));
        public string ProductNumber { get; set; }
        public int? ProductDim { get; set; }
        public string CustomerNumber { get; set; }
        public int? CustomerDim { get; set; }
        [DbColumnMap("ActualPriceFact")]
        public decimal ActualPrice { get; set; }
    }

    private static DateTime GetMaxOrderDate() =>
        SqlTask.ExecuteScalar<DateTime>(OLTP_Connection, "SELECT MAX(OrderDate) FROM Orders");


    private static void AddOrders() {
        SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Orders VALUES(20001,'2023-01-04', 'P-00013', 'C-1001', 1555)");
        SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Orders VALUES(20002,'2023-01-04', 'P-00010', 'C-1002', 288)");
        SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Orders VALUES(20003,'2023-01-04', 'P-00011', 'C-1003', 689)");
    }

    private static void AddOrderWithFlawedRecord() {
        SqlTask.ExecuteNonQuery(OLTP_Connection, "INSERT INTO Orders VALUES(30002,'2023-01-06', 'P-XXXXX', 'C-1002', 1999)");
    }



    private static void LoadOrders(DateTime lastLoadDate) {
        var source = new DbSource<Order>(OLTP_Connection);
        source.SqlParameter = new[] {
            new QueryParameter() { Name = "date", Value = lastLoadDate }
        };
        source.Sql = @"SELECT OrderId, OrderDate, ProductNumber, CustomerNumber, ActualPrice
                           FROM Orders
                           WHERE OrderDate > @date";

        var dest = new DbDestination<Order>(OLAP_Connection, "FactOrders");

        var customerLookup = new LookupTransformation<Order, Customer>();
        var customerLookupSource = new DbSource<Customer>(OLAP_Connection, "DimCustomer");
        customerLookup.MatchColumns = new[] {
            new MatchColumn() {
                InputPropertyName = nameof(Order.CustomerNumber),
                LookupSourcePropertyName = nameof(Customer.Number)
            }
        };
        customerLookup.RetrieveColumns= new[] {
            new RetrieveColumn() {
                InputPropertyName = nameof(Order.CustomerDim),
                LookupSourcePropertyName = nameof(Customer.DimId)
            }
        };
        customerLookup.Source = customerLookupSource;

        var productLookup = new LookupTransformation<Order, Product>();
        var productLookupSource = new DbSource<Product>(OLAP_Connection, "DimProduct");
        productLookup.MatchColumns = new[] {
            new MatchColumn() {
                InputPropertyName = nameof(Order.ProductNumber),
                LookupSourcePropertyName = nameof(Product.Number)
            }
        };
        productLookup.RetrieveColumns = new[] {
            new RetrieveColumn() {
                InputPropertyName = nameof(Order.ProductDim),
                LookupSourcePropertyName = nameof(Product.DimId)
            }
        };
        productLookup.Source = productLookupSource;

        var errorLog = new JsonDestination<ETLBoxError>("errros.log");

        source.LinkTo(customerLookup);
        customerLookup.LinkTo(productLookup);
        productLookup.LinkTo(dest);
                    
        dest.LinkErrorTo(errorLog);

        Network.Execute(source);
    }

    #endregion

}