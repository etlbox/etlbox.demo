using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DbExtensions;
using ETLBox.SqlServer;
using Microsoft.Data.SqlClient;
using System.Net.Http.Headers;

string connectionString = @"Data Source=np:\\.\pipe\LOCALDB#4BDFE241\tsql\query;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;";
var connection = new SqlConnection(connectionString);
var connectionManager = new SqlConnectionManager(connectionString);

PrepareTableWithData();
SimpleBulkDelete();
WithOptionsBulkDelete();

void PrepareTableWithData() {

    var tableDefinition = TableDefinition.FromCLRType(ConnectionType.SqlServer, typeof(Customer));
    DropTableTask.DropIfExists(connectionManager, tableDefinition.Name);
    CreateTableTask.CreateIfNotExists(connectionManager, tableDefinition);
    var customers = Enumerable.Range(1, 4_999)
        .Select(i => new Customer { Id = i, Name = $"Customer {i}", City = $"City {i % 50}" });

    connection.BulkInsert(customers);
    Console.WriteLine($"Prepared table '{tableDefinition.Name}' - filled with 5000 rows.");
}

void SimpleBulkDelete() {

    Console.WriteLine("Simple bulk delete of 2500 customer rows.");
    var customers = Enumerable.Range(1000, 1000)
        .Select(i => new Customer { Id = i })
        .Union(
            Enumerable.Range(3000, 1500)
            .Select(i => new Customer { Id = i })
        );

    connection.BulkDelete(customers);
    Console.WriteLine("Bulk delete of 2500 rows completed successfully.");
}

void WithOptionsBulkDelete() {
    Console.WriteLine("Bulk delete with options.");
    var customers = Enumerable.Range(2_000, 500)
        .Select(i => new Customer { Name = $"Customer {i}" });

    connection.BulkDelete(customers, options => {
        options.BatchSize = 50;
        options.IdColumns = new[] { new IdColumn() { IdPropertyName = "Name" } };
        options.BeforeBatchWrite = (batch) => {
            Console.WriteLine($"Before batch with {batch.Length} rows.");
            return batch;
        };
    });
    Console.WriteLine("Bulk delete with options of 500 rows completed successfully.");
}

public class Customer {
    [IdColumn]
    public int Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }
}


