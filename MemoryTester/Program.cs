using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.SqlServer;

namespace MemoryTester;

public static class Program {
    /* Configuration*/
    public static string DatabaseName => "demo";
    public static int BatchSize = 1000;
    public static string ConnectionString = $"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;";

    private static long LastInsertedId = 0;
    private static long _currentUpdateId = 0;
    private static long _currentDeleteId = 0;
    private static long _currentInsertId = 0;
    private static Random Random = new Random();

    public static async Task Main() {
        string input;
        ETLBox.Licensing.LicenseService.CurrentKey =
                @"2026-03-28|TRIAL|ONLY FOR PERSONAL OR TESTING PURPOSES|CUSTOMER:Support|MAIL:support@etlbox.net||cTl4SRAqvqBggIPO9G44fH+wfDUV4wYg5oV7NTbFo6zxIkBKAwrEFEMSzudJYGtblbrETxCRkxNidtM6jprLNra9XPiYtYzFf+lh7iXua9JY0857DVrCwDHAayNONrzpXvSmF5WK5BOa8klV5+bqeks1kT9zCshnhCEB8JNZHmU=";

        do {
            Console.WriteLine("\n--- MemoryTester Menu ---");
            Console.WriteLine("create | load100k | load10m | load100m | merge100k | merge10m | merge100m | gc | exit");
            Console.Write("> ");
            input = Console.ReadLine()?.Trim().ToLower() ?? "";

            Console.WriteLine();

            switch (input) {
                case "create":
                    CreateTargetTable();
                    break;
                case "load100k":
                    await LoadDataAsync(100_000);
                    break;
                case "load10m":
                    await LoadDataAsync(10_000_000);
                    break;
                case "load100m":
                    await LoadDataAsync(100_000_000);
                    break;
                case "merge100k":
                    await MergeDataAsync(100_000);
                    break;
                case "merge10m":
                    await MergeDataAsync(10_000_000);
                    break;
                case "merge100m":
                    await MergeDataAsync(100_000_000);
                    break;
                case "gc":
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    Console.WriteLine("Garbage Collection completed.");
                    PrintDiagnostics();
                    break;
                case "exit":
                    return;
                default:
                    if (!string.IsNullOrEmpty(input)) {
                        Console.WriteLine("Invalid option.");
                    }
                    break;
            }
        } while (input != "exit");
    }

    private static void CreateTargetTable() {
        try {
            using var connection = GetSqlConnection();
            DropTableTask.DropIfExists(connection, "TargetTable");

            var tableDefinition = new TableDefinition("TargetTable",
                new List<TableColumn>()
                {
                    new TableColumn("Id", "BIGINT", allowNulls: false, isPrimaryKey: true),
                    new TableColumn("LongValue1", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue2", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue3", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue4", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue5", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue6", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue7", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue8", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue9", "BIGINT", allowNulls: false),
                    new TableColumn("LongValue10", "BIGINT", allowNulls: false),
                    new TableColumn("StringValue1", "VARCHAR(100)", allowNulls: true),
                    new TableColumn("StringValue2", "VARCHAR(100)", allowNulls: true),
                    new TableColumn("StringValue3", "VARCHAR(100)", allowNulls: true),
                    new TableColumn("StringValue4", "VARCHAR(100)", allowNulls: true),
                    new TableColumn("StringValue5", "VARCHAR(100)", allowNulls: true),
                    new TableColumn("DeleteFlag", "BIT", allowNulls: false),
                     new TableColumn("ChangeAction", "BIT", allowNulls: false),
                });

            CreateTableTask.Create(connection, tableDefinition);
            Console.WriteLine("✓ TargetTable successfully created.");
            PrintDiagnostics();
        } catch (Exception ex) {
            Console.WriteLine($"✗ Error creating table: {ex.Message}");
        }
    }

    private static async Task LoadDataAsync(int recordCount) {
        try {
            Console.WriteLine($"Loading {recordCount:N0} records...");
            using var connection = GetSqlConnection();

            var source = new CustomBatchSource<DbRow>();
            source.ReadBatchFunc = ProduceLoadBatch;
            source.ReadingCompleted = (progressCount) => progressCount >= recordCount;

            var dest = new DbDestination<DbRow>(connection, "TargetTable");

            source.LinkTo(dest);

            await ExecuteWithLiveDiagnostics(new Network(source), $"Loading {recordCount:N0} records");

            Console.WriteLine($"✓ Load completed. Total records inserted: {recordCount:N0}");
            PrintDiagnostics();
        } catch (Exception ex) {
            Console.WriteLine($"✗ Error during load: {ex.Message}");
        }

        IEnumerable<DbRow> ProduceLoadBatch(int progressCount) {
            var batch = new List<DbRow>();
            for (long i = LastInsertedId; i < LastInsertedId + BatchSize; i++) {
                batch.Add(GenerateDbRow(i));
            }
            LastInsertedId += BatchSize;
            return batch;
        }
    }

    private static async Task MergeDataAsync(int recordCount) {
        try {
            // Read maximum ID from database first
            long maxIdInDatabase = GetMaxIdFromTable("TargetTable");

            int updateCount = (int)(recordCount * 0.3);  // 30%
            int deleteCount = (int)(recordCount * 0.1);  // 10%
            int insertCount = recordCount - updateCount - deleteCount; // 60%

            // Adjust counts if database has fewer records than expected
            long existingRecords = maxIdInDatabase;
            if (existingRecords < updateCount + deleteCount) {
                int adjustedUpdateCount = Math.Max(0, (int)(existingRecords * 0.75));
                int adjustedDeleteCount = Math.Max(0, (int)(existingRecords * 0.25));
                int adjustedInsertCount = recordCount - adjustedUpdateCount - adjustedDeleteCount;
                
                Console.WriteLine($"Database has only {existingRecords:N0} records. Adjusting distribution:");
                Console.WriteLine($"  - Updates: {adjustedUpdateCount:N0}");
                Console.WriteLine($"  - Deletes: {adjustedDeleteCount:N0}");
                Console.WriteLine($"  - Inserts: {adjustedInsertCount:N0}");
                
                updateCount = adjustedUpdateCount;
                deleteCount = adjustedDeleteCount;
                insertCount = adjustedInsertCount;
            }

            // Initialize ID counters - start in the middle of the existing range if possible
            long updateDeleteRangeSize = updateCount + deleteCount;
            long availableRange = Math.Max(1, maxIdInDatabase - updateDeleteRangeSize);
            long rangeStartOffset = availableRange / 2;  // Start roughly in the middle
            
            _currentUpdateId = Math.Max(1, rangeStartOffset);
            _currentDeleteId = _currentUpdateId + updateCount;
            _currentInsertId = maxIdInDatabase + 1;

            Console.WriteLine($"Starting Merge with {recordCount:N0} records (DB has {existingRecords:N0} existing records):");
            Console.WriteLine($"  - Updates: {updateCount:N0} (IDs {_currentUpdateId:N0} to {_currentUpdateId + updateCount - 1:N0})");
            Console.WriteLine($"  - Deletes: {deleteCount:N0} (IDs {_currentDeleteId:N0} to {_currentDeleteId + deleteCount - 1:N0})");
            Console.WriteLine($"  - Inserts: {insertCount:N0} (IDs {_currentInsertId:N0} onwards)");

            using var connection = GetSqlConnection();

            var source = new CustomBatchSource<DbRow>();
            source.ReadBatchFunc = ProduceMergeBatch;
            source.ReadingCompleted = (progressCount) => progressCount >= recordCount;

            var dest = new DbMerge<DbRow>(connection, "TargetTable") {
                MergeMode = MergeMode.Delta,
                CacheMode = CacheMode.Partial,
            };
            dest.FindDuplicates = false;

            source.LinkTo(dest);

            await ExecuteWithLiveDiagnostics(new Network(source), $"Merging {recordCount:N0} records");

            Console.WriteLine($"✓ Merge completed.");
            PrintDiagnostics();
        } catch (Exception ex) {
            Console.WriteLine($"✗ Error during merge: {ex.Message}");
        }

        IEnumerable<DbRow> ProduceMergeBatch(int progressCount) {
            var batch = new List<DbRow>();
            
            // Calculate distribution per batch
            int updatePerBatch = (int)(BatchSize * 0.3);      // 300 per 1000
            int deletePerBatch = (int)(BatchSize * 0.1);      // 100 per 1000
            int insertPerBatch = BatchSize - updatePerBatch - deletePerBatch;  // 600 per 1000

            // Generate updates (continuous IDs)
            for (int i = 0; i < updatePerBatch; i++) {
                batch.Add(GenerateDbRow(_currentUpdateId));
                _currentUpdateId++;
            }

            // Generate deletes (continuous IDs)
            for (int i = 0; i < deletePerBatch; i++) {
                var row = GenerateDbRow(_currentDeleteId);
                row.DeleteFlag = true;
                batch.Add(row);
                _currentDeleteId++;
            }

            // Generate inserts (continuous IDs)
            for (int i = 0; i < insertPerBatch; i++) {
                batch.Add(GenerateDbRow(_currentInsertId));
                _currentInsertId++;
            }

            return batch;
        }
    }

    private static DbRow GenerateDbRow(long id) {
        return new DbRow {
            Id = id,
            LongValue1 = Random.NextInt64(),
            LongValue2 = Random.NextInt64(),
            LongValue3 = Random.NextInt64(),
            LongValue4 = Random.NextInt64(),
            LongValue5 = Random.NextInt64(),
            LongValue6 = Random.NextInt64(),
            LongValue7 = Random.NextInt64(),
            LongValue8 = Random.NextInt64(),
            LongValue9 = Random.NextInt64(),
            LongValue10 = Random.NextInt64(),
            StringValue1 = GenerateRandomString(10),
            StringValue2 = GenerateRandomString(15),
            StringValue3 = GenerateRandomString(20),
            StringValue4 = GenerateRandomString(25),
            StringValue5 = GenerateRandomString(30),
        };
    }

    private static string GenerateRandomString(int length) {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        return new string(Enumerable.Range(0, length)
            .Select(_ => chars[Random.Next(chars.Length)])
            .ToArray());
    }

    private static async Task ExecuteWithLiveDiagnostics(Network network, string operationName) {
        var cts = new CancellationTokenSource();

        // Start diagnostics reporting task
        var diagnosticsTask = Task.Run(() => {
            while (!cts.Token.IsCancellationRequested) {
                PrintProgressLine(operationName);
                Thread.Sleep(500); // Update every 500ms
            }
        });

        try {
            // Execute network
            await network.ExecuteAsync(cts.Token);
        } finally {
            cts.Cancel();
            await diagnosticsTask;
            Console.WriteLine(); // New line after diagnostics
        }
    }

    private static void PrintProgressLine(string operationName) {
        long memory = GC.GetTotalMemory(false) / 1024 / 1024;
        int targetRowCount = GetRowCount("TargetTable");

        // Use carriage return to overwrite the same line
        Console.Write($"\r{operationName} | Memory: {memory:N0} MB | Rows in DB: {targetRowCount:N0}        ");
    }

    private static void PrintDiagnostics() {
        long memory = GC.GetTotalMemory(false) / 1024 / 1024;
        int targetRowCount = GetRowCount("TargetTable");

        Console.WriteLine();
        Console.WriteLine("=== Diagnostics ===");
        Console.WriteLine($"TargetTable Row Count: {targetRowCount:N0}");
        Console.WriteLine($"Managed Heap Memory: {memory:N0} MB");
        Console.WriteLine("===================");
    }

    private static int GetRowCount(string tableName) {
        try {
            using var connection = GetSqlConnection();
            return new RowCountTask() {
                ConnectionManager = connection,
                TableName = tableName,
                Options = RowCountOptions.QuickQueryMode
            }.Count();
        } catch {
            return 0;
        }
    }

    private static long GetMaxIdFromTable(string tableName) {
        try {
            using var connection = GetSqlConnection();
            var task = new SqlTask() {
                ConnectionManager = connection,
                Sql = $"SELECT ISNULL(MAX(Id), 0) FROM {tableName}"
            };
            return task.ExecuteScalar<long>();
        } catch {
            return 0;
        }
    }

    private static SqlConnectionManager GetSqlConnection() =>
        new(ConnectionString);
}
