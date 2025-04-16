using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DbExtensions;
using ETLBox.SqlServer;
using Microsoft.Data.SqlClient;
using System.Net.Http.Headers;

string connectionString = @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;";
var connection = new SqlConnection(connectionString);
var connectionManager = new SqlConnectionManager(connectionString);

PrepareTableWithData();
SimpleBulkUpdate();
WithOptionsBulkUpdate();

void PrepareTableWithData() {

    var tableDefinition = TableDefinition.FromCLRType(ConnectionType.SqlServer, typeof(Customer));
    DropTableTask.DropIfExists(connectionManager, tableDefinition.Name);
    CreateTableTask.CreateIfNotExists(connectionManager, tableDefinition);
    var customers = Enumerable.Range(1, 4_999)
        .Select(i => new Customer { Id = i, Name = $"Customer {i}", City = $"City {i % 50}" });

    connection.BulkInsert(customers);
    Console.WriteLine($"Prepared table '{tableDefinition.Name}' - filled with 5000 rows.");
}

void SimpleBulkUpdate() {

    Console.WriteLine("Simple bulk update of 2500 customer rows.");
    var customers = Enumerable.Range(1, 2_500)
        .Select(i => new Customer { 
            Id = i, 
            Name = $"Updated {i}", 
            City = $"City {i % 25}" 
        });

    connection.BulkUpdate(customers);
    Console.WriteLine("Bulk update of 2500 rows completed successfully.");
}

void WithOptionsBulkUpdate() {
    Console.WriteLine("Bulk update with options.");
    var customers = Enumerable.Range(2_000, 2_500)
        .Select(i => new Customer { Id = i, Name = $"Options update {i}", City = $"My City {i % 25}" });

    connection.BulkUpdate(customers, options => {
        options.BatchSize = 500;
        options.UpdateColumns = new[] { new UpdateColumn() { UpdatePropertyName = "Name" } };
        options.BeforeBatchWrite = (batch) => {
            Console.WriteLine($"Before batch with {batch.Length} rows.");
            return batch;
        };
    });
    Console.WriteLine("Bulk update with options of 2500 rows completed successfully.");
}

public class Customer {
    [IdColumn]
    public int Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }
}


