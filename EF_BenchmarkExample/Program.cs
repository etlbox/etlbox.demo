using EFBox.SqlServer;
using System.Diagnostics;

int NumberOfBenchmarkRowsInsert = 100_000;
int NumberOfBenchmarkRowsUpdate = 20_000;
int NumberOfBenchmarkRowsMerge = 20_000;

CheckIfDatabaseExists("efbenchmark");

Console.WriteLine("Running benchmarks for BulkInsert/BulkUpdate/BulkDelete");


Console.WriteLine("Running benchmark for Bulk Inserts");
RefreshBenchmarkTable("BenchmarkRows");
BulkInsertEntityFramework();
RefreshBenchmarkTable("BenchmarkRows");
BulkInsertEFBox();

Console.WriteLine("Running benchmark for Bulk Update and Delete");
RefreshBenchmarkTable("BenchmarkRows");
BulkUpdateAndDeleteEntityFramework();
RefreshBenchmarkTable("BenchmarkRows");
BulkUpdateAndDeleteEFBox();

Console.WriteLine("Running benchmark for BulkMerge");
RefreshBenchmarkTable("BenchmarkRows");
BulkMergeEFBox();
RefreshBenchmarkTable("BenchmarkRows");
BulkMergeEntityFramework();




void BulkInsertEFBox() {
    using (var db = new BenchmarkDbContext()) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsInsert);
        MeasureExecutionTime("Benchmark EFBox BulkInsert", () => {
            db.BulkInsert(benchmarkRows);

        });
    }
}

void BulkInsertEntityFramework() {
    using (var db = new BenchmarkDbContext()) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsInsert);
        db.BenchmarkRows.AddRange(benchmarkRows);
        MeasureExecutionTime("Benchmark Entitfy Framework SaveChanges", () => {
            db.SaveChanges();
        });
    }
}



void BulkUpdateAndDeleteEFBox() {
    using (var db = new BenchmarkDbContext()) {
        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);

        string updateValue = RandomString(1024);
        db.BulkInsert(benchmarkRows);

        foreach (var b in benchmarkRows) {
            b.Value1 = updateValue;
            b.Value2 = updateValue;
            b.Value3 = updateValue;
            b.Value4 = updateValue;
            b.Value5 = updateValue;
            b.Value6 = updateValue;
            b.Value7 = updateValue;
            b.Value8 = updateValue;
            b.Value9 = updateValue;
            b.Value10 = updateValue;
        }

        MeasureExecutionTime("Benchmark EFBox BulkUpdate", () => {
            db.BulkUpdate(benchmarkRows);
        });

        MeasureExecutionTime("Benchmark EFBox BulkDelete", () => {
            db.BulkDelete(benchmarkRows);
        });
    }
}


void BulkUpdateAndDeleteEntityFramework() {
    using (var db = new BenchmarkDbContext()) {
        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);
        db.BenchmarkRows.AddRange(benchmarkRows);
        db.SaveChanges();

        string updateValue = RandomString(1024);
        foreach (var b in db.BenchmarkRows) {
            b.Value1 = updateValue;
            b.Value2 = updateValue;
            b.Value3 = updateValue;
            b.Value4 = updateValue;
            b.Value5 = updateValue;
            b.Value6 = updateValue;
            b.Value7 = updateValue;
            b.Value8 = updateValue;
            b.Value9 = updateValue;
            b.Value10 = updateValue;
        }


        MeasureExecutionTime("Benchmark Entity Framework BulkUpdate", () => {
            db.SaveChanges();
        });

        db.BenchmarkRows.RemoveRange(db.BenchmarkRows);
        MeasureExecutionTime("Benchmark Entity Framework BulkDelete", () => {
            db.SaveChanges();
        });
    }
}


void BulkMergeEFBox() {   

    using (var db = new BenchmarkDbContext()) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsMerge*2);        
        db.BulkInsert(benchmarkRows);

        benchmarkRows.RemoveRange(0, NumberOfBenchmarkRowsMerge);
        string updateValue = RandomString(1024);
        for (int i = 0; i < NumberOfBenchmarkRowsMerge; i++) {
            benchmarkRows[i].Value1 = updateValue;
            benchmarkRows[i].Value2 = updateValue;
            benchmarkRows[i].Value3 = updateValue;
            benchmarkRows[i].Value4 = updateValue;
            benchmarkRows[i].Value5 = updateValue;
            benchmarkRows[i].Value6 = updateValue;
            benchmarkRows[i].Value7 = updateValue;
            benchmarkRows[i].Value8 = updateValue;
            benchmarkRows[i].Value9 = updateValue;
            benchmarkRows[i].Value10 = updateValue;
        }

        var benchmarkRowsNew = CreateBenchmarkRows(NumberOfBenchmarkRowsMerge);
        benchmarkRows.AddRange(benchmarkRowsNew);

        MeasureExecutionTime("Benchmark EFBox BulkMerge", () => {
            db.BulkMerge(benchmarkRows);
        });
    }
}


void BulkMergeEntityFramework() {

    using (var db = new BenchmarkDbContext()) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsMerge * 2);
        db.BenchmarkRows.AddRange(benchmarkRows);
        db.SaveChanges();
    
        foreach (var entity in db.BenchmarkRows.Take(NumberOfBenchmarkRowsMerge).ToArray())
            db.Remove(entity);    
        string updateValue = RandomString(1024);
        foreach (var row in db.BenchmarkRows.Skip(NumberOfBenchmarkRowsMerge)) {
            row.Value1 = updateValue;
            row.Value2 = updateValue;
            row.Value3 = updateValue;
            row.Value4 = updateValue;
            row.Value5 = updateValue;
            row.Value6 = updateValue;
            row.Value7 = updateValue;
            row.Value8 = updateValue;
            row.Value9 = updateValue;
            row.Value10 = updateValue;
        }

        var benchmarkRowsNew = CreateBenchmarkRows(NumberOfBenchmarkRowsMerge);
        db.BenchmarkRows.AddRange(benchmarkRowsNew);

        MeasureExecutionTime("Benchmark Entity Framework BulkMerge", () => {
            db.SaveChanges();
        });
    }
}

static void MeasureExecutionTime(string description, Action action) {
    Console.WriteLine($"Running: {description}");
    Stopwatch s = new Stopwatch();
    s.Start();
    action.Invoke();
    s.Stop();
    Console.WriteLine($"Done - time elapsed: {s.Elapsed.TotalSeconds}sec");
}



List<BenchmarkRow> CreateBenchmarkRows(int numberOfRows) {
    var result = new List<BenchmarkRow>();
    for (int i = 1; i <= numberOfRows; i++) {
        string randomString = RandomString(128);
        result.Add(new BenchmarkRow() {
            Value1 = "i" + randomString,
            Value2 = "i" + randomString,
            Value3 = "i" + randomString,
            Value4 = "i" + randomString,
            Value5 = "i" + randomString,
            Value6 = "i" + randomString,
            Value7 = "i" + randomString,
            Value8 = "i" + randomString,
            Value9 = "i" + randomString,
            Value10 = "i" + randomString
        });
    }
    return result;
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






