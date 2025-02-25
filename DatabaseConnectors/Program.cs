using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Postgres;

//Setting up test data

// Create a PostgreSQL connection
var conn = new PostgresConnectionManager("Server=localhost;Database=demo;User Id=postgres;Password=etlboxpassword;");
Settings.DefaultDbConnection = conn;

// Create and populate source table
SqlTask.ExecuteNonQuery(@"DROP TABLE IF EXISTS orders");
SqlTask.ExecuteNonQuery(@"CREATE TABLE orders (
  ""OrderId"" SERIAL PRIMARY KEY,
  ""CustomerName"" VARCHAR(100) NOT NULL,
  ""OrderAmount"" DECIMAL(10,2) NOT NULL
 )");
SqlTask.ExecuteNonQuery(@"INSERT INTO orders (""CustomerName"", ""OrderAmount"") VALUES 
  ('Alice', 100.50), 
  ('Bob', 200.75), 
  ('Charlie', 150.00)");

// Create target tables
SqlTask.ExecuteNonQuery(@"DROP TABLE IF EXISTS order_archive");
SqlTask.ExecuteNonQuery(@"CREATE TABLE order_archive (
  ""OrderId"" INT PRIMARY KEY,
  ""CustomerName"" VARCHAR(100) NOT NULL,
  ""OrderAmount"" DECIMAL(10,2) NOT NULL
 )");

SqlTask.ExecuteNonQuery(@"INSERT INTO order_archive (""OrderId"", ""CustomerName"", ""OrderAmount"") VALUES 
  (0, 'Archived', 100)");

SqlTask.ExecuteNonQuery(@"DROP TABLE IF EXISTS merged_orders");
SqlTask.ExecuteNonQuery(@"CREATE TABLE merged_orders (
  ""OrderId"" INT PRIMARY KEY,
  ""CustomerName"" VARCHAR(100) NOT NULL,
  ""OrderAmount"" DECIMAL(10,2) NOT NULL
 )");

SqlTask.ExecuteNonQuery(@"INSERT INTO merged_orders (""OrderId"", ""CustomerName"", ""OrderAmount"") VALUES 
  (0, 'ToDelete', 100),
  (1, 'ToUpdate', 100)");

// Read data from source table
var source = new DbSource<OrderData>(conn, "orders");

// Create multicast component to duplicate data
var multicast = new Multicast<OrderData>();

// Define first destination: simple insert (archive orders)
var archiveDest = new DbDestination<OrderData>(conn, "order_archive");

// Define second destination: full merge (synchronize orders)
var mergeDest = new DbMerge<OrderData>(conn, "merged_orders") {
    MergeMode = MergeMode.Full,
    CacheMode = CacheMode.Partial
};

// Link components
source.LinkTo(multicast);
multicast.LinkTo(archiveDest);
multicast.LinkTo(mergeDest);

// Execute the network
Network.Execute(source);


public class OrderData : MergeableRow {
    [DbColumnMap("OrderId")]
    [IdColumn]
    public int Id { get; set; }

    [DbColumnMap("CustomerName")]
    [CompareColumn]
    public string Name { get; set; }

    [DbColumnMap("OrderAmount")]
    [CompareColumn]
    public decimal Amount { get; set; }
}
