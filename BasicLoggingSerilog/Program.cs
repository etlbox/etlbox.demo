using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using Microsoft.Extensions.Logging;
using Serilog;
using BasicLoggingSerilog;
using Serilog.Extensions.Logging;

Settings.DefaultDbConnection = new SqlConnectionManager("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;");


var serilogLogger = new LoggerConfiguration()
    .Enrich.FromLogContext()    
    .MinimumLevel.Debug()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3} {taskName}] {Message:lj}"+Environment.NewLine)
    .CreateLogger();

Settings.LogInstance = new SerilogLoggerFactory(serilogLogger).CreateLogger("Default");

RecreateDatabase("demo", Settings.DefaultDbConnection.ConnectionString as SqlConnectionString);
PrepareDatabase();


var dbsource = new DbSource<Order>("OrderTable");

var lookup = new LookupTransformation<Order, Customer>();
lookup.Source = new DbSource<Customer>("CustomerTable");
lookup.RetrieveColumns = new[] {
    new RetrieveColumn() { InputPropertyName = "CustomerId", LookupSourcePropertyName = "Id" }
};
lookup.MatchColumns = new[] {
    new MatchColumn() { InputPropertyName = "CustomerName", LookupSourcePropertyName = "Name" }
};

var agg = new Aggregation<Order, QuantityByCustomer>();
agg.GroupColumns = new[] {
    new GroupColumn() { GroupPropNameInInput = "CustomerId", GroupPropNameInOutput = "CustomerId" }
};
agg.AggregateColumns = new[] {
    new AggregateColumn() { InputValuePropName = "Quantity", AggregatedValuePropName = "Quantity", AggregationMethod = AggregationMethod.Sum }
};

var dbdest = new DbDestination<QuantityByCustomer>("QuantityByCustomer");


dbsource.LinkTo(lookup);
lookup.LinkTo(agg);
agg.LinkTo(dbdest);
Network.Execute(dbsource); //Run the data flow

void RecreateDatabase(string dbName, SqlConnectionString connectionString) {
    var masterConnection = new SqlConnectionManager(connectionString.CloneWithMasterDbName());
    DropDatabaseTask.DropIfExists(masterConnection, dbName);
    CreateDatabaseTask.Create(masterConnection, dbName);
}

void PrepareDatabase() {

    DropTableTask.DropIfExists("OrderTable");
    var tdOrder = new TableDefinition("OrderTable"
        , new List<TableColumn>() {
                new TableColumn("Id", "INT", allowNulls: false, isIdentity:true, isPrimaryKey:true),
                new TableColumn("Item", "VARCHAR(100)", allowNulls: false),
                new TableColumn("Quantity", "INT", allowNulls: false),
                new TableColumn("CustomerName", "VARCHAR(100)", allowNulls: true),
    });
    tdOrder.CreateTable();


    SqlTask.ExecuteNonQuery("INSERT INTO OrderTable (Item, Quantity, CustomerName) VALUES ('Jeans',1,'John')");
    SqlTask.ExecuteNonQuery("INSERT INTO OrderTable (Item, Quantity, CustomerName) VALUES ('Shirt',2,'Jim')");
    SqlTask.ExecuteNonQuery("INSERT INTO OrderTable (Item, Quantity, CustomerName) VALUES ('Watch',1,NULL)");
    SqlTask.ExecuteNonQuery("INSERT INTO OrderTable (Item, Quantity, CustomerName) VALUES ('Shoes',3,'John')");
    SqlTask.ExecuteNonQuery("INSERT INTO OrderTable (Item, Quantity, CustomerName) VALUES ('Belt',1,'Jim')");

    DropTableTask.DropIfExists("CustomerTable");
    var tdCust = new TableDefinition("CustomerTable"
        , new List<TableColumn>() {
                new TableColumn("Id", "INT", allowNulls: false),
                new TableColumn("Name", "NVARCHAR(100)", allowNulls: true)
    });
    tdCust.CreateTable();

    SqlTask.ExecuteNonQuery("INSERT INTO CustomerTable VALUES (1,'John')");
    SqlTask.ExecuteNonQuery("INSERT INTO CustomerTable VALUES (2,'Jim')");

    DropTableTask.DropIfExists("QuantityByCustomer");
    var tdQuantityByCust = new TableDefinition("QuantityByCustomer"
        , new List<TableColumn>() {
                new TableColumn("Id", "INT", allowNulls: false, isIdentity:true, isPrimaryKey:true),                
                new TableColumn("Quantity", "INT", allowNulls: false),
                new TableColumn("CustomerId", "INT", allowNulls: true),
    });
    tdQuantityByCust.CreateTable();
}


