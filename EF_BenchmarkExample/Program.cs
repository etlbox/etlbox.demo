using ETLBox.DataFlow;
using ETLBox.EntityFramework.SqlServer;
using System.Diagnostics;

int NumberOfBenchmarkRows = 100_000;
CheckIfDatabaseExists("efbenchmark");

RefreshBenchmarkTable("BenchmarkRows");

Console.WriteLine("Running benchmarks for BulkInsert/BulkUpdate/BulkDelete");
using (var db = new BenchmarkDbContext()) {

    var benchmarkRows = new List<BenchmarkRow>();
    for (int i = 1; i <= NumberOfBenchmarkRows; i++) {
        benchmarkRows.Add(new BenchmarkRow() {
            Value1 = RandomString(1024),
            Value2 = RandomString(1024),
            Value3 = RandomString(1024),
            Value4 = RandomString(1024),
            Value5 = RandomString(1024),
            Value6 = RandomString(1024),
            Value7 = RandomString(1024),
            Value8 = RandomString(1024),
            Value9 = RandomString(1024),
            Value10 = RandomString(1024)
        });
    }
    MeasureExecutionTime("Benchmark EFBox BulkInsert", () => {
        db.BulkInsert(benchmarkRows);
    });

    foreach (var b in benchmarkRows) {
        b.Value1 = RandomString(1024);
        b.Value2 = RandomString(1024);
        b.Value3 = RandomString(1024);
        b.Value4 = RandomString(1024);
        b.Value5 = RandomString(1024);
        b.Value6 = RandomString(1024);
        b.Value7 = RandomString(1024);
        b.Value8 = RandomString(1024);
        b.Value9 = RandomString(1024);
        b.Value10 = RandomString(1024);
    }

    MeasureExecutionTime("Benchmark EFBox BulkUpdate", () => {
        db.BulkUpdate(benchmarkRows);
    });

    MeasureExecutionTime("Benchmark EFBox BulkDelete", () => {
        db.BulkDelete(benchmarkRows);
    });
}

Console.WriteLine();
Console.WriteLine("Running benchmark for BulkMerge "); 

RefreshBenchmarkTable("BenchmarkRows");

using (var db = new BenchmarkDbContext()) {

    var benchmarkRows = new List<BenchmarkRow>();
    for (int i = 1; i <= NumberOfBenchmarkRows*2; i++) {
        benchmarkRows.Add(new BenchmarkRow() {
            Value1 = RandomString(1024),
            Value2 = RandomString(1024),
            Value3 = RandomString(1024),
            Value4 = RandomString(1024),
            Value5 = RandomString(1024),
            Value6 = RandomString(1024),
            Value7 = RandomString(1024),
            Value8 = RandomString(1024),
            Value9 = RandomString(1024),
            Value10 = RandomString(1024)
        });
    }
    db.BulkInsert(benchmarkRows);

    benchmarkRows.RemoveRange(0, NumberOfBenchmarkRows);
    for (int i=0;i<NumberOfBenchmarkRows;i++) {
        benchmarkRows[i].Value1 = RandomString(1024);
        benchmarkRows[i].Value2 = RandomString(1024);
        benchmarkRows[i].Value3 = RandomString(1024);
        benchmarkRows[i].Value4 = RandomString(1024);
        benchmarkRows[i].Value5 = RandomString(1024);
        benchmarkRows[i].Value6 = RandomString(1024);
        benchmarkRows[i].Value7 = RandomString(1024);
        benchmarkRows[i].Value8 = RandomString(1024);
        benchmarkRows[i].Value9 = RandomString(1024);
        benchmarkRows[i].Value10 = RandomString(1024);
    }

    for (int i = 1; i <= NumberOfBenchmarkRows; i++) {
        benchmarkRows.Add(new BenchmarkRow() {
            Value1 = RandomString(1024),
            Value2 = RandomString(1024),
            Value3 = RandomString(1024),
            Value4 = RandomString(1024),
            Value5 = RandomString(1024),
            Value6 = RandomString(1024),
            Value7 = RandomString(1024),
            Value8 = RandomString(1024),
            Value9 = RandomString(1024),
            Value10 = RandomString(1024)
        });
    }
    MeasureExecutionTime("Benchmark EFBox BulkMerge", () => {
        db.BulkMerge(benchmarkRows);
    });
}

static void MeasureExecutionTime(string description, Action action) {
    Console.WriteLine($"Running: {description}");
    Stopwatch s = new Stopwatch();
    s.Start();
    action.Invoke();
    s.Stop();
    Console.WriteLine($"Done - time elapsed: {s.Elapsed.TotalSeconds}sec");
}


static string RandomString(int length) {
    Random random = new Random();
    const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    return new string(Enumerable.Repeat(chars, length)
        .Select(s => s[random.Next(s.Length)]).ToArray());
}

void CheckIfDatabaseExists(string dbName) {
    var connDb = new SqlConnectionManager(BenchmarkDbContext.ConnectionString);
    var dbExists = IfDatabaseExistsTask.IsExisting(BenchmarkDbContext.SqlConnectionManager, dbName);
    if (!dbExists)
        throw new Exception($"A database {dbName} was not found - please create database first!");
}


void RefreshBenchmarkTable(string tableName) {
    Console.WriteLine("Preparing database - refreshing benchmark table");
    DropTableTask.DropIfExists(BenchmarkDbContext.SqlConnectionManager, tableName);
    CreateTableTask.CreateIfNotExists(BenchmarkDbContext.SqlConnectionManager,
        tableName,
        new List<TableColumn>() {
            new TableColumn() {
                Name = "BenchmarkRowId",
                DataType = "INT",
                AllowNulls = false,
                IsPrimaryKey = true,
                IsIdentity = true
            },
            new TableColumn() { Name = "Value1", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value2", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value3", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value4", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value5", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value6", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value7", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value8", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value9", DataType = "VARCHAR(1024)", AllowNulls = true },
            new TableColumn() { Name = "Value10", DataType = "VARCHAR(1024)", AllowNulls = true },
    });
}






