using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.DbExtensions;
using ETLBox.SqlServer;
using Microsoft.Data.SqlClient;

string connectionString = @"Data Source=np:\\.\pipe\LOCALDB#4BDFE241\tsql\query;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;";
var connection = new SqlConnection(connectionString);
var connectionManager = new SqlConnectionManager(connectionString);

PrepareTableWithData();
SimpleBulkMerge();
WithOptionsBulkMerge();

void PrepareTableWithData() {

    var tableDefinition = TableDefinition.FromCLRType(ConnectionType.SqlServer, typeof(Customer));
    DropTableTask.DropIfExists(connectionManager, tableDefinition.Name);
    CreateTableTask.CreateIfNotExists(connectionManager, tableDefinition);
    var customers = Enumerable.Range(1, 2_500)
        .Select(i => new Customer { Id = i, Name = $"Customer {i}", City = $"City {i % 50}" });

    connection.BulkInsert(customers);
    Console.WriteLine($"Prepared table '{tableDefinition.Name}' - filled with 5000 rows.");
}

void SimpleBulkMerge() {

    Console.WriteLine("Simple bulk merge example.");
    var customers = Enumerable.Range(1, 1_500)
        .Select(i => new Customer { Id = i, Name = $"Update Customer {i}", City = $"City {i % 50}" })
        .Union(
            Enumerable.Range(3_000, 1500)
            .Select(i => new Customer { Id = i, Name = $"New Customer {i}", City = $"City {i % 50}" })
        );

    connection.BulkMerge(customers);
    Console.WriteLine("Bulk merge completed successfully.");
}

void WithOptionsBulkMerge() {
    Console.WriteLine("Bulk merge with options.");

    var customers = Enumerable.Range(1, 1_500)
       .Select(i => new Customer { Id = i, Name = $"Options Customer {i}", City = $"New City {i % 50}" })
       .Union(
           Enumerable.Range(5_000, 500)
           .Select(i => new Customer { Id = i, Name = $"New Customer {i}", City = $"New City {i % 50}" })
       );


    connection.BulkMerge(customers, options => {
        options.ReadGeneratedValues = true;
        options.CompareColumns = new[] { new CompareColumn() { ComparePropertyName = "City" } };
        options.UpdateColumns = new[] { new UpdateColumn() { UpdatePropertyName = "City" } };
        options.MergeMode = MergeMode.Delta;
    });
    Console.WriteLine("Bulk merge with options completed successfully.");
    Console.WriteLine("Max id of inserted data:" + customers.Max(c => c.Id));
}

public class Customer : IMergeableRow {
    [IdColumn]
    public int Id { get; set; }    
    public string Name { get; set; }
    public string City { get; set; }
    public DateTime ChangeDate { get; set; }
    [DbColumnDefinition(DataType = "INT")]
    public ChangeAction? ChangeAction { get; set; }
}


