using Dapper;
using ETLBox;
using ETLBox.DbExtensions;
using Microsoft.Data.SqlClient;

using var connection = new SqlConnection("Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true");

BulkInsert();
BulkUpdate();
BulkDelete();
BulkMerge();

void BulkInsert() {
    // Create table with Dapper
    connection.Execute(@"
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customer')
    CREATE TABLE Customer (
        Id INT PRIMARY KEY,
        Name NVARCHAR(100),
        City NVARCHAR(100)
    )");


    var customers = Enumerable.Range(1, 5_000)
    .Select(i => new Customer {
        Id = i,
        Name = $"Name {i}",
        City = $"City {i % 50}"
    });

    connection.BulkInsert(customers);

    //connection.BulkInsert(customers, options => {
    //    options.BatchSize = 500;
    //    options.TablePrefix = "dim";
    //    options.ReadGeneratedValues = true;
    //    options.OnProgress = progress => {
    //        if (progress % 1000 == 0)
    //            Console.WriteLine($"Inserted {progress} rows.");
    //    };
    //});
}

void BulkUpdate() {
    var customers = Enumerable.Range(1, 2_500)
    .Select(i => new Customer {
        Id = i,
        Name = $"Updated {i}",
        City = $"City {i % 25}"
    });

    connection.BulkUpdate(customers);

    //connection.BulkUpdate(customers, options => {
    //    options.BatchSize = 500;
    //    options.UpdateColumns = new[] { new UpdateColumn() { UpdatePropertyName = "Name" } };
    //    options.BeforeBatchWrite = (batch) => {
    //        Console.WriteLine($"Updating batch with {batch.Length} rows.");
    //        return batch;
    //    };
    //});
}

void BulkDelete() {
    var customers = Enumerable.Range(1000, 1000)
    .Select(i => new Customer { Id = i });

    connection.BulkDelete(customers);

    //connection.BulkDelete(customers, options => {
    //    options.BatchSize = 500;
    //    options.IdColumns = new[] { new IdColumn() { IdPropertyName = "Name" } };
    //    options.BeforeBatchWrite = (batch) => {
    //        Console.WriteLine($"Deleting batch with {batch.Length} rows.");
    //        return batch;
    //    };
    //});
}

void BulkMerge() {
    var customers = Enumerable.Range(1, 1_500)
    .Select(i => new Customer { Id = i, Name = $"Update Customer {i}", City = $"City {i % 50}" })
    .Union(
        Enumerable.Range(3_000, 1500)
        .Select(i => new Customer { Id = i, Name = $"New Customer {i}", City = $"City {i % 50}" })
    );

    connection.BulkMerge(customers);

    //connection.BulkMerge(customers, options => {
    //    options.MergeMode = MergeMode.Delta;
    //    options.CompareColumns = new[] { new CompareColumn() { ComparePropertyName = "City" } };
    //    options.UpdateColumns = new[] { new UpdateColumn() { UpdatePropertyName = "City" } };
    //    options.ReadGeneratedValues = true;
    //});
}
public class Customer : IMergeableRow {
    [IdColumn]
    public int Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }

    public DateTime ChangeDate { get; set; }
    public ChangeAction? ChangeAction { get; set; }
}