using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DbExtensions;
using ETLBox.SqlServer;
using Microsoft.Data.SqlClient;

string connectionString = @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;";
var connection = new SqlConnection(connectionString);
var connectionManager = new SqlConnectionManager(connectionString);


SimpleBulkInsert();
WithOptionsBulkInsert();

void SimpleBulkInsert() {

    var tableDefinition = TableDefinition.FromCLRType(ConnectionType.SqlServer, typeof(Customer));
    DropTableTask.DropIfExists(connectionManager, tableDefinition.Name);
    CreateTableTask.CreateIfNotExists(connectionManager, tableDefinition);
    Console.WriteLine($"Prepared table '{tableDefinition.Name}'.");

    Console.WriteLine("Simple bulk insert of 5000 customer rows.");
    var customers = Enumerable.Range(1, 4_999)
        .Select(i => new Customer { Id = i, Name = $"Customer {i}", City = $"City {i % 50}" });

    connection.BulkInsert(customers);
    Console.WriteLine("Bulk inserted completed successfully.");
}

void WithOptionsBulkInsert() {
    var tableDefinition = TableDefinition.FromCLRType(ConnectionType.SqlServer, typeof(Customer));
    tableDefinition.Name = "dimCustomer";
    DropTableTask.DropIfExists(connectionManager, tableDefinition.Name);
    CreateTableTask.CreateIfNotExists(connectionManager, tableDefinition);
    Console.WriteLine($"Prepared table '{tableDefinition.Name}'.");

    Console.WriteLine("Bulk insert with options of 5000 customer rows.");
    var customers = Enumerable.Range(1, 4_999)
        .Select(i => new Customer { Id = i, Name = $"Customer {i}", City = $"City {i % 50}" });

    connection.BulkInsert(customers, options => {
        options.BatchSize = 500;
        options.TablePrefix = "dim";
        options.ReadGeneratedValues = true;
        options.OnProgress = progress => {
            if (progress % 1000 == 0)
                Console.WriteLine($"Inserted {progress} rows.");
        };
    });
    Console.WriteLine("Bulk inserted with options completed successfully.");
    Console.WriteLine("Max id of inserted data:" + customers.Max(c => c.Id));
}

public class Customer {
    public int Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }
}


