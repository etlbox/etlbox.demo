using EFBox.SqlServer;
using ETLBox.SqlServer;
using System.Diagnostics;

int NumberOfBenchmarkRowsInsert = 100_000;
int NumberOfBenchmarkRowsUpdate = 10_000;
int NumberOfBenchmarkRowsMerge = 10_000;

ConnectionType usedConnectionType = ConnectionType.SqlServer;

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
BulkMergeEntityFramework();
RefreshBenchmarkTable("BenchmarkRows");
BulkMergeEFBox();





void BulkInsertEFBox() {
    using (var db = new BenchmarkDbContext(usedConnectionType)) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsInsert);
        MeasureExecutionTime("  EFBox BulkInsert", () => {
            db.BulkInsert(benchmarkRows);
        });
    }
}

void BulkInsertEntityFramework() {
    using (var db = new BenchmarkDbContext(usedConnectionType)) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsInsert);
        db.BenchmarkRows.AddRange(benchmarkRows);
        MeasureExecutionTime("  Entity Framework Bulk Insert", () => {
            db.SaveChanges();
        });
    }
}



void BulkUpdateAndDeleteEFBox() {
    using (var db = new BenchmarkDbContext(usedConnectionType)) {
        var clutterBefore = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);
        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);
        var clutterAfter = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);

        db.BulkInsert(clutterBefore);
        db.BulkInsert(benchmarkRows);
        db.BulkInsert(clutterAfter);

        string updateValue = RandomString(1024);
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

        MeasureExecutionTime("  EFBox BulkUpdate", () => {
            db.BulkUpdate(benchmarkRows);
        });

        MeasureExecutionTime("  EFBox BulkDelete", () => {
            db.BulkDelete(benchmarkRows);
        });
    }
}


void BulkUpdateAndDeleteEntityFramework() {
    using (var db = new BenchmarkDbContext(usedConnectionType)) {
        var clutterBefore = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);
        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);
        var clutterAfter = CreateBenchmarkRows(NumberOfBenchmarkRowsUpdate);
        
        db.BulkInsert(clutterBefore);
        db.BenchmarkRows.AddRange(benchmarkRows);
        db.SaveChanges();
        db.BulkInsert(clutterAfter);

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


        MeasureExecutionTime("  Entity Framework BulkUpdate", () => {
            db.SaveChanges();
        });

        db.BenchmarkRows.RemoveRange(db.BenchmarkRows);
        MeasureExecutionTime("  Entity Framework BulkDelete", () => {
            db.SaveChanges();
        });
    }
}


void BulkMergeEFBox() {   

    using (var db = new BenchmarkDbContext(usedConnectionType)) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsMerge*3);        
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

        MeasureExecutionTime("  EFBox BulkMerge", () => {
            db.BulkMerge(benchmarkRows);
        });
    }
}


void BulkMergeEntityFramework() {

    using (var db = new BenchmarkDbContext(usedConnectionType)) {

        var benchmarkRows = CreateBenchmarkRows(NumberOfBenchmarkRowsMerge * 3);
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

        MeasureExecutionTime("  Entity Framework BulkMerge", () => {
            db.SaveChanges();
        });
    }
}

static void MeasureExecutionTime(string description, Action action) {
    Console.WriteLine($"  Running: {description}");
    Stopwatch s = new Stopwatch();
    s.Start();
    action.Invoke();
    s.Stop();
    Console.WriteLine($"  Done - time elapsed: {s.Elapsed.TotalSeconds}sec");
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
    var dbExists = IfDatabaseExistsTask.IsExisting(GetConnectionManager(), dbName);
    if (!dbExists)
        throw new Exception($"A database {dbName} was not found - please create database first!");
}


void RefreshBenchmarkTable(string tableName) {
    DropTableTask.DropIfExists(GetConnectionManager(), tableName);
    CreateTableTask.CreateIfNotExists(GetConnectionManager(),
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

IConnectionManager GetConnectionManager() {
    return usedConnectionType switch {
        ConnectionType.SqlServer => new SqlConnectionManager(BenchmarkDbContext.SqlConnectionString),
        //ConnectionType.Postgres => new PostgresConnectionManager(BenchmarkDbContext.PostgresConnectionString),
        //ConnectionType.MySql => new MySqlConnectionManager(BenchmarkDbContext.MySqlConnectionString),
        _ => throw new NotSupportedException()
    };
}




//Some benchmarks:

/* SqlServer:
 * Running benchmarks for BulkInsert/BulkUpdate/BulkDelete
Running benchmark for Bulk Inserts
  Running:   Entity Framework Bulk Insert
  Done - time elapsed: 31,5161238sec
  Running:   EFBox BulkInsert
  Done - time elapsed: 7,4981721sec
Running benchmark for Bulk Update and Delete
  Running:   Entity Framework BulkUpdate
  Done - time elapsed: 12,7123246sec
  Running:   Entity Framework BulkDelete
  Done - time elapsed: 2,7331631sec
  Running:   EFBox BulkUpdate
  Done - time elapsed: 7,0074806sec
  Running:   EFBox BulkDelete
  Done - time elapsed: 0,8207653sec
Running benchmark for BulkMerge
  Running:   Entity Framework BulkMerge
  Done - time elapsed: 12,5751387sec
  Running:   EFBox BulkMerge
  Done - time elapsed: 9,8358188sec
*/

 
/* MySql:
Running benchmarks for BulkInsert/BulkUpdate/BulkDelete
Running benchmark for Bulk Inserts
  Running:   Entity Framework Bulk Insert
  Done - time elapsed: 22,2421894sec
  Running:   EFBox BulkInsert
  Done - time elapsed: 16,0939463sec
Running benchmark for Bulk Update and Delete
  Running:   Entity Framework BulkUpdate
  Done - time elapsed: 17,3836979sec
  Running:   Entity Framework BulkDelete
  Done - time elapsed: 26,8939086sec
  Running:   EFBox BulkUpdate
  Done - time elapsed: 3,1929282sec
  Running:   EFBox BulkDelete
  Done - time elapsed: 0,2094562sec
Running benchmark for BulkMerge
  Running:   Entity Framework BulkMerge
  Done - time elapsed: 15,388673sec
  Running:   EFBox BulkMerge
  Done - time elapsed: 11,2574562sec

*/

/*
 * Postgres:
 * Running benchmarks for BulkInsert/BulkUpdate/BulkDelete
Running benchmark for Bulk Inserts
  Running:   Entity Framework Bulk Insert
  Done - time elapsed: 13,5590612sec
  Running:   EFBox BulkInsert
  Done - time elapsed: 26,4066426sec
Running benchmark for Bulk Update and Delete
  Running:   Entity Framework BulkUpdate
  Done - time elapsed: 7,8382902sec
  Running:   Entity Framework BulkDelete
  Done - time elapsed: 2,5075012sec
  Running:   EFBox BulkUpdate
  Done - time elapsed: 3,9414873sec
  Running:   EFBox BulkDelete
  Done - time elapsed: 1,091278sec
Running benchmark for BulkMerge
  Running:   Entity Framework BulkMerge
  Done - time elapsed: 4,6927466sec
  Running:   EFBox BulkMerge
  Done - time elapsed: 10,3513316sec
*/