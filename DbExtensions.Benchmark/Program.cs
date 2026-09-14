using System.Data;
using System.Diagnostics;
using Dapper;
using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DbExtensions;
using ETLBox.SqlServer;
using Microsoft.Data.SqlClient;

/*
 * Compare Dapper row-by-row writes with ETLBox.DbExtensions bulk operations
 * (and SqlBulkCopy for inserts) on SQL Server.
 *
 *   dotnet run -c Release
 *   dotnet run -c Release -- --rows 50000 --cs "Data Source=localhost;..."
 *
 * Without a license key, keep --rows at or below 4999 (trial limit).
 * Place etlbox.lic next to this project to measure larger sets.
 *
 * Connection string (first match wins):
 *   --cs "..."
 *   env DBEXTENSIONS_BENCHMARK_CS
 *   default: localhost / sa / demo  (same as the other DbExtensions demos)
 */

int rows = 5_000;
string? connectionString = Environment.GetEnvironmentVariable("DBEXTENSIONS_BENCHMARK_CS");
bool skipLoop = false;

for (int i = 0; i < args.Length; i++) {
    switch (args[i]) {
        case "--rows" when i + 1 < args.Length:
            rows = int.Parse(args[++i]);
            break;
        case "--cs" when i + 1 < args.Length:
            connectionString = args[++i];
            break;
        case "--skip-loop":
            skipLoop = true;
            break;
        case "--help":
        case "-h":
            PrintHelp();
            return;
    }
}

connectionString ??=
    @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;";

const string tableName = "DbExtBenchmarkCustomer";
var results = new List<Result>();

using var connection = new SqlConnection(connectionString);
var connectionManager = new SqlConnectionManager(connectionString);

Console.WriteLine($"SQL Server benchmark  |  rows={rows}  |  skip-loop={skipLoop}");
Console.WriteLine($"Connection: {MaskConnectionString(connectionString)}");
Console.WriteLine();

EnsureDatabase(connection);
RecreateTable(connectionManager, tableName);

Warmup(connection, connectionManager, tableName);

BenchmarkInsert(connection, connectionManager, tableName, rows, skipLoop, results);
BenchmarkUpdate(connection, connectionManager, tableName, rows, skipLoop, results);
BenchmarkDelete(connection, connectionManager, tableName, rows, skipLoop, results);
BenchmarkMerge(connection, connectionManager, tableName, rows, skipLoop, results);

PrintSummary(results, rows);

static void PrintHelp() {
    Console.WriteLine("""
        DbExtensions.Benchmark — Dapper row-by-row vs ETLBox.DbExtensions bulk ops

        Options:
          --rows <n>     Row count (default 5000; stay ≤4999 without a license)
          --cs "<str>"   SQL Server connection string
          --skip-loop    Skip Dapper foreach baselines (useful for very large n)
          --help         Show this text

        Env:
          DBEXTENSIONS_BENCHMARK_CS   Connection string if --cs is omitted
        """);
}

static void EnsureDatabase(SqlConnection connection) {
    connection.Open();
    connection.Close();
}

static void RecreateTable(SqlConnectionManager connectionManager, string tableName) {
    DropTableTask.DropIfExists(connectionManager, tableName);
    CreateTableTask.Create(connectionManager, new TableDefinition(tableName, [
        new TableColumn("Id", "INT", allowNulls: false, isPrimaryKey: true),
        new TableColumn("Name", "NVARCHAR(100)"),
        new TableColumn("City", "NVARCHAR(100)")
    ]));
}

static void Truncate(SqlConnectionManager connectionManager, string tableName) {
    TruncateTableTask.Truncate(connectionManager, tableName);
}

static void Warmup(SqlConnection connection, SqlConnectionManager connectionManager, string tableName) {
    Console.WriteLine("Warmup…");
    var warmup = CreateCustomers(1, 200);
    connection.BulkInsert(warmup, o => o.TableName = tableName);
    Truncate(connectionManager, tableName);
}

static void BenchmarkInsert(
    SqlConnection connection,
    SqlConnectionManager connectionManager,
    string tableName,
    int rows,
    bool skipLoop,
    List<Result> results) {

    Console.WriteLine($"Insert ({rows:N0} rows)");
    var data = CreateCustomers(1, rows).ToList();

    if (!skipLoop) {
        Truncate(connectionManager, tableName);
        results.Add(Measure("Insert", "Dapper (row by row)", rows, () => {
            foreach (var row in data)
                connection.Execute(
                    $"INSERT INTO {tableName} (Id, Name, City) VALUES (@Id, @Name, @City)",
                    row);
        }));
    }

    Truncate(connectionManager, tableName);
    results.Add(Measure("Insert", "SqlBulkCopy", rows, () => {
        InsertSqlBulkCopy(connection, tableName, data);
    }));

    Truncate(connectionManager, tableName);
    results.Add(Measure("Insert", "BulkInsert", rows, () => {
        connection.BulkInsert(data, o => o.TableName = tableName);
    }));
}

static void BenchmarkUpdate(
    SqlConnection connection,
    SqlConnectionManager connectionManager,
    string tableName,
    int rows,
    bool skipLoop,
    List<Result> results) {

    Console.WriteLine($"Update ({rows:N0} rows)");
    var seed = CreateCustomers(1, rows).ToList();
    var updated = seed.Select(c => new Customer {
        Id = c.Id,
        Name = $"Updated {c.Id}",
        City = $"City {c.Id % 25}"
    }).ToList();

    void Seed() {
        Truncate(connectionManager, tableName);
        connection.BulkInsert(seed, o => o.TableName = tableName);
    }

    if (!skipLoop) {
        Seed();
        results.Add(Measure("Update", "Dapper (row by row)", rows, () => {
            foreach (var row in updated)
                connection.Execute(
                    $"UPDATE {tableName} SET Name = @Name, City = @City WHERE Id = @Id",
                    row);
        }));
    }

    Seed();
    results.Add(Measure("Update", "BulkUpdate", rows, () => {
        connection.BulkUpdate(updated, o => o.TableName = tableName);
    }));
}

static void BenchmarkDelete(
    SqlConnection connection,
    SqlConnectionManager connectionManager,
    string tableName,
    int rows,
    bool skipLoop,
    List<Result> results) {

    Console.WriteLine($"Delete ({rows:N0} rows)");
    var seed = CreateCustomers(1, rows).ToList();
    var toDelete = seed.Select(c => new Customer { Id = c.Id }).ToList();

    void Seed() {
        Truncate(connectionManager, tableName);
        connection.BulkInsert(seed, o => o.TableName = tableName);
    }

    if (!skipLoop) {
        Seed();
        results.Add(Measure("Delete", "Dapper (row by row)", rows, () => {
            foreach (var row in toDelete)
                connection.Execute($"DELETE FROM {tableName} WHERE Id = @Id", row);
        }));
    }

    Seed();
    results.Add(Measure("Delete", "BulkDelete", rows, () => {
        connection.BulkDelete(toDelete, o => o.TableName = tableName);
    }));
}

static void BenchmarkMerge(
    SqlConnection connection,
    SqlConnectionManager connectionManager,
    string tableName,
    int rows,
    bool skipLoop,
    List<Result> results) {

    int existing = rows;
    int incomingUpdates = rows / 2;
    int incomingInserts = rows / 2;
    int incoming = incomingUpdates + incomingInserts;
    Console.WriteLine($"Merge ({incoming:N0} incoming rows against {existing:N0} existing)");

    var seed = CreateCustomers(1, existing).ToList();
    var incomingRows = CreateCustomers(1, incomingUpdates)
        .Select(c => new Customer {
            Id = c.Id,
            Name = $"Merged {c.Id}",
            City = "Merged City"
        })
        .Concat(CreateCustomers(existing + 1, incomingInserts))
        .ToList();

    void Seed() {
        Truncate(connectionManager, tableName);
        connection.BulkInsert(seed, o => o.TableName = tableName);
    }

    if (!skipLoop) {
        Seed();
        results.Add(Measure("Merge", "Dapper (select + insert/update)", incoming, () => {
            foreach (var row in incomingRows) {
                var exists = connection.ExecuteScalar<int>(
                    $"SELECT COUNT(1) FROM {tableName} WHERE Id = @Id",
                    row) > 0;
                if (exists)
                    connection.Execute(
                        $"UPDATE {tableName} SET Name = @Name, City = @City WHERE Id = @Id",
                        row);
                else
                    connection.Execute(
                        $"INSERT INTO {tableName} (Id, Name, City) VALUES (@Id, @Name, @City)",
                        row);
            }
        }));
    }

    Seed();
    results.Add(Measure("Merge", "BulkMerge", incoming, () => {
        connection.BulkMerge(incomingRows, o => {
            o.TableName = tableName;
            o.MergeMode = MergeMode.InsertsAndUpdates;
        });
    }));
}

static void InsertSqlBulkCopy(SqlConnection connection, string tableName, List<Customer> data) {
    bool close = false;
    if (connection.State != ConnectionState.Open) {
        connection.Open();
        close = true;
    }

    using var bulk = new SqlBulkCopy(connection);
    bulk.DestinationTableName = tableName;
    bulk.BatchSize = 5000;
    bulk.ColumnMappings.Add(nameof(Customer.Id), "Id");
    bulk.ColumnMappings.Add(nameof(Customer.Name), "Name");
    bulk.ColumnMappings.Add(nameof(Customer.City), "City");

    var table = new DataTable();
    table.Columns.Add("Id", typeof(int));
    table.Columns.Add("Name", typeof(string));
    table.Columns.Add("City", typeof(string));
    foreach (var row in data)
        table.Rows.Add(row.Id, row.Name, row.City);

    bulk.WriteToServer(table);
    if (close)
        connection.Close();
}

static Result Measure(string operation, string method, int rows, Action action) {
    Console.Write($"  {method,-36}");
    var sw = Stopwatch.StartNew();
    action();
    sw.Stop();
    Console.WriteLine($"{sw.Elapsed.TotalSeconds,8:0.000} s");
    return new Result(operation, method, rows, sw.Elapsed.TotalSeconds);
}

static IEnumerable<Customer> CreateCustomers(int startId, int count) =>
    Enumerable.Range(startId, count).Select(i => new Customer {
        Id = i,
        Name = $"Customer {i}",
        City = $"City {i % 50}"
    });

static void PrintSummary(List<Result> results, int rows) {
    Console.WriteLine();
    Console.WriteLine("Markdown (copy into the docs):");
    Console.WriteLine();
    Console.WriteLine($"SQL Server, {rows:N0} rows, Dapper row-by-row vs ETLBox.DbExtensions.");
    Console.WriteLine();
    Console.WriteLine("| Operation | Method | Seconds | vs Dapper |");
    Console.WriteLine("| --- | --- | ---: | ---: |");

    foreach (var group in results.GroupBy(r => r.Operation)) {
        var dapper = group.FirstOrDefault(r => r.Method.StartsWith("Dapper", StringComparison.Ordinal));
        foreach (var r in group) {
            string factor = dapper is null || r.Method.StartsWith("Dapper", StringComparison.Ordinal)
                ? "—"
                : $"{dapper.Seconds / r.Seconds:0.0}×";
            Console.WriteLine($"| {r.Operation} | {r.Method} | {r.Seconds:0.000} | {factor} |");
        }
    }
}

static string MaskConnectionString(string cs) {
    try {
        var builder = new SqlConnectionStringBuilder(cs);
        if (!string.IsNullOrEmpty(builder.Password))
            builder.Password = "***";
        return builder.ToString();
    }
    catch {
        return "(connection string)";
    }
}

sealed record Result(string Operation, string Method, int Rows, double Seconds);

public class Customer : IMergeableRow {
    [IdColumn]
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";

    [DbColumnDefinition(Ignore = true)]
    public DateTime ChangeDate { get; set; }
    [DbColumnDefinition(Ignore = true)]
    public ChangeAction? ChangeAction { get; set; }
}
