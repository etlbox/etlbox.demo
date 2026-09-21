using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using SsisToCodeFirst;

// Adjust the connection string to your local database
string sqlConnectionString =
    @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true";

var connectionManager = new SqlConnectionManager(sqlConnectionString);

PrepareDatabase(connectionManager);

TruncateTableTask.Truncate(connectionManager, "StagingOrders");

var source = new CsvSource<OrderRow>("orders.csv");

var normalize = new RowTransformation<OrderRow>(row => {
    row.CustomerCode = row.CustomerCode?.Trim().ToUpperInvariant();
    row.ProductSku = row.ProductSku?.Trim().ToUpperInvariant();
    return row;
});

var customerLookup = new LookupTransformation<OrderRow, Customer> {
    Source = new DbSource<Customer>(connectionManager, "Customers")
};

var priceLookup = new LookupTransformation<OrderRow, ProductPrice> {
    Source = new DbSource<ProductPrice>(connectionManager, "ProductPrices"),
    PermitMultipleEntriesPerKey = true,
    ApplyRetrievedCacheToInput = (order, cache) => {
        var price = cache.List.FirstOrDefault(p =>
            p.Sku == order.ProductSku
            && order.OrderDate >= p.ValidFrom
            && order.OrderDate <= p.ValidTo);
        order.UnitPrice = price?.Price;
        return order;
    }
};

var staging = new DbDestination<OrderRow>(connectionManager, "StagingOrders");
var errors = new CsvDestination<OrderRow>("errors.csv");

source.LinkTo(normalize);
normalize.LinkTo(customerLookup);
customerLookup.LinkTo(priceLookup);
priceLookup.LinkTo(staging, row => row.IsComplete);
priceLookup.LinkTo(errors, row => !row.IsComplete);

await Network.ExecuteAsync(source);

SqlTask.ExecuteNonQuery(connectionManager, @"
    MERGE INTO Orders AS dest
    USING StagingOrders AS src
    ON dest.OrderId = src.OrderId
    WHEN MATCHED THEN
        UPDATE SET
            OrderDate = src.OrderDate,
            CustomerId = src.CustomerId,
            CustomerName = src.CustomerName,
            ProductSku = src.ProductSku,
            Quantity = src.Quantity,
            UnitPrice = src.UnitPrice
    WHEN NOT MATCHED THEN
        INSERT (OrderId, OrderDate, CustomerId, CustomerName, ProductSku, Quantity, UnitPrice)
        VALUES (src.OrderId, src.OrderDate, src.CustomerId, src.CustomerName, src.ProductSku, src.Quantity, src.UnitPrice);
");

int staged = SqlTask.ExecuteScalar<int>(connectionManager, "SELECT COUNT(*) FROM StagingOrders");
int loaded = SqlTask.ExecuteScalar<int>(connectionManager, "SELECT COUNT(*) FROM Orders");
Console.WriteLine($"Staged {staged} orders. Orders table now has {loaded} rows. Errors: {errors.ProgressCount}");

Console.WriteLine("Errors:");
Console.WriteLine(File.ReadAllText("errors.csv"));
Console.WriteLine("Press ENTER to exit.");

Console.ReadLine();
static void PrepareDatabase(SqlConnectionManager connectionManager)
{
    CreateTableTask.CreateIfNotExists(connectionManager, "Customers",
        new List<TableColumn> {
            new TableColumn("Id", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
            new TableColumn("Code", "VARCHAR(20)", allowNulls: false),
            new TableColumn("Name", "VARCHAR(100)", allowNulls: false)
        });

    CreateTableTask.CreateIfNotExists(connectionManager, "ProductPrices",
        new List<TableColumn> {
            new TableColumn("Sku", "VARCHAR(50)", allowNulls: false),
            new TableColumn("Price", "DECIMAL(10,2)", allowNulls: false),
            new TableColumn("ValidFrom", "DATE", allowNulls: false),
            new TableColumn("ValidTo", "DATE", allowNulls: false)
        });

    var orderColumns = new List<TableColumn> {
        new TableColumn("OrderId", "INT", allowNulls: false, isPrimaryKey: true),
        new TableColumn("OrderDate", "DATE", allowNulls: false),
        new TableColumn("CustomerId", "INT", allowNulls: false),
        new TableColumn("CustomerName", "VARCHAR(100)", allowNulls: true),
        new TableColumn("ProductSku", "VARCHAR(50)", allowNulls: false),
        new TableColumn("Quantity", "INT", allowNulls: false),
        new TableColumn("UnitPrice", "DECIMAL(10,2)", allowNulls: false)
    };
    CreateTableTask.CreateIfNotExists(connectionManager, "StagingOrders", orderColumns);
    CreateTableTask.CreateIfNotExists(connectionManager, "Orders", orderColumns);

    TruncateTableTask.Truncate(connectionManager, "Customers");
    TruncateTableTask.Truncate(connectionManager, "ProductPrices");

    SqlTask.ExecuteNonQuery(connectionManager, "INSERT INTO Customers (Code, Name) VALUES ('ACME', 'Acme Corp')");
    SqlTask.ExecuteNonQuery(connectionManager, "INSERT INTO Customers (Code, Name) VALUES ('BETA', 'Beta GmbH')");
    SqlTask.ExecuteNonQuery(connectionManager,
        "INSERT INTO ProductPrices (Sku, Price, ValidFrom, ValidTo) VALUES ('WIDGET-A', 9.99, '2025-01-01', '2026-02-28')");
    SqlTask.ExecuteNonQuery(connectionManager,
        "INSERT INTO ProductPrices (Sku, Price, ValidFrom, ValidTo) VALUES ('WIDGET-A', 12.50, '2026-03-01', '9999-12-31')");
    SqlTask.ExecuteNonQuery(connectionManager,
        "INSERT INTO ProductPrices (Sku, Price, ValidFrom, ValidTo) VALUES ('WIDGET-B', 4.00, '2025-01-01', '9999-12-31')");
}
