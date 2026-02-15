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
    public static int MaxBufferSize = 20_000;
    public static string ConnectionString = $"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;";

    #region Private Variables
    private static long LastInsertedId = 0;
    private static long _recordsGenerated = 0;
    private static long _currentUpdateId = 0;
    private static long _currentDeleteId = 0;
    private static long _currentInsertId = 0;
    private static long _updatesRemaining = 0;
    private static long _deletesRemaining = 0;
    private static long _insertsRemaining = 0;
    private static long _minMemoryConsumption = long.MaxValue;
    private static long _maxMemoryConsumption = 0;
    private static long _totalMemoryConsumption = 0;
    private static long _memoryMeasurementCount = 0;
    private static DateTimeOffset _operationStartTime = DateTimeOffset.MinValue;
    private static Random Random = new Random();
    #endregion

    public static async Task Main(string[] args) {
        string input;
        ETLBox.Licensing.LicenseService.CurrentKey =
                @"2026-03-28|TRIAL|ONLY FOR PERSONAL OR TESTING PURPOSES|CUSTOMER:Support|MAIL:support@etlbox.net||cTl4SRAqvqBggIPO9G44fH+wfDUV4wYg5oV7NTbFo6zxIkBKAwrEFEMSzudJYGtblbrETxCRkxNidtM6jprLNra9XPiYtYzFf+lh7iXua9JY0857DVrCwDHAayNONrzpXvSmF5WK5BOa8klV5+bqeks1kT9zCshnhCEB8JNZHmU=";

        ETLBox.Settings.MaxBufferSize = MaxBufferSize;

        do {
            Console.WriteLine("\n--- MemoryTester Menu ---");
            Console.WriteLine("create | load100k | load1m | load10m | load100m | merge100k | merge1m | merge10m | merge100m | gc | exit");
            Console.WriteLine($"Current Settings: BatchSize={BatchSize:N0}, MaxBufferSize={MaxBufferSize:N0}");
            Console.WriteLine("Settings: batchsize=<value> | maxbuffersize=<value>");
            Console.Write("> ");
            input = Console.ReadLine()?.Trim().ToLower() ?? "";

            Console.WriteLine();

            if (input.Contains("=")) {
                HandleSettingChange(input);
                continue;
            }

            switch (input) {
                case "create":
                    CreateTargetTable();
                    break;
                case "load100k":
                    await LoadDataAsync(100_000);
                    break;
                case "load1m":
                    await LoadDataAsync(1_000_000);
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
                case "merge1m":
                    await MergeDataAsync(1_000_000);
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

    private static void HandleSettingChange(string input) {
        var parts = input.Split('=');
        if (parts.Length != 2) {
            Console.WriteLine("✗ Invalid format. Use: key=value");
            return;
        }

        string key = parts[0].Trim();
        string value = parts[1].Trim();

        if (key.Equals("batchsize", StringComparison.OrdinalIgnoreCase)) {
            if (int.TryParse(value, out int batchSize) && batchSize > 0) {
                BatchSize = batchSize;
                Console.WriteLine($"✓ BatchSize set to {BatchSize:N0}");
            } else {
                Console.WriteLine($"✗ Invalid BatchSize value: {value}");
            }
        } else if (key.Equals("maxbuffersize", StringComparison.OrdinalIgnoreCase)) {
            if (int.TryParse(value, out int maxBufferSize) && maxBufferSize > 0) {
                MaxBufferSize = maxBufferSize;
                ETLBox.Settings.MaxBufferSize = MaxBufferSize;
                Console.WriteLine($"✓ MaxBufferSize set to {MaxBufferSize:N0}");
            } else {
                Console.WriteLine($"✗ Invalid MaxBufferSize value: {value}");
            }
        } else {
            Console.WriteLine($"✗ Unknown setting: {key}");
        }
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
                    new TableColumn("ChangeAction", "INT", allowNulls: true),
                    new TableColumn("ChangeDate", "DATETIME", allowNulls: true),
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
            ResetMemoryTracking();
            _recordsGenerated = 0;

            LastInsertedId = GetMaxIdFromTable("TargetTable");

            Console.WriteLine($"Loading {recordCount:N0} records starting from ID {LastInsertedId + 1:N0}...");
            using var connection = GetSqlConnection();

            var source = new CustomBatchSource<DbRow>();
            source.ReadBatchFunc = ProduceLoadBatch;
            source.ReadingCompleted = (progressCount) => _recordsGenerated >= recordCount;

            var dest = new DbDestination<DbRow>(connection, "TargetTable");
            dest.BatchSize = BatchSize;

            source.LinkTo(dest);

            await ExecuteWithLiveDiagnostics(new Network(source), $"Loading {recordCount:N0} records");

            Console.WriteLine($"✓ Load completed. Total records inserted: {_recordsGenerated:N0}");
            PrintDiagnostics();
        } catch (Exception ex) {
            Console.WriteLine($"✗ Error during load: {ex.Message}");
        }

        IEnumerable<DbRow> ProduceLoadBatch(int progressCount) {
            var batch = new List<DbRow>();

            // Berechne wie viele Records in dieser Batch generiert werden sollen
            long recordsToGenerate = Math.Min(BatchSize, recordCount - _recordsGenerated);

            for (long i = 0; i < recordsToGenerate; i++) {
                LastInsertedId++;
                batch.Add(GenerateDbRow(LastInsertedId));
            }

            _recordsGenerated += batch.Count;
            return batch;
        }
    }

    private static async Task MergeDataAsync(int recordCount) {
        try {
            ResetMemoryTracking();

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

            // Setze remaining Counts
            _updatesRemaining = updateCount;
            _deletesRemaining = deleteCount;
            _insertsRemaining = insertCount;

            using var connection = GetSqlConnection();

            var source = new CustomBatchSource<DbRow>();
            source.ReadBatchFunc = ProduceMergeBatch;
            source.ReadingCompleted = (progressCount) => progressCount >= recordCount;

            var dest = new DbMerge<DbRow>(connection, "TargetTable") {
                MergeMode = MergeMode.Delta,
                CacheMode = CacheMode.Partial,
            };
            dest.FindDuplicates = false;
            dest.BatchSize = BatchSize;

            source.LinkTo(dest);

            await ExecuteWithLiveDiagnostics(new Network(source), $"Merging {recordCount:N0} records");

            Console.WriteLine($"✓ Merge completed.");
            PrintDiagnostics();
        } catch (Exception ex) {
            Console.WriteLine($"✗ Error during merge: {ex.Message}");
        }

        IEnumerable<DbRow> ProduceMergeBatch(int progressCount) {
            var batch = new List<DbRow>();

            // Generate updates (continuous IDs)
            while (batch.Count < BatchSize && _updatesRemaining > 0) {
                batch.Add(GenerateDbRow(_currentUpdateId));
                _currentUpdateId++;
                _updatesRemaining--;
            }

            // Generate deletes (continuous IDs)
            while (batch.Count < BatchSize && _deletesRemaining > 0) {
                var row = GenerateDbRow(_currentDeleteId);
                row.DeleteFlag = true;
                batch.Add(row);
                _currentDeleteId++;
                _deletesRemaining--;
            }

            // Generate inserts (continuous IDs)
            while (batch.Count < BatchSize && _insertsRemaining > 0) {
                batch.Add(GenerateDbRow(_currentInsertId));
                _currentInsertId++;
                _insertsRemaining--;
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

        // Track min/max/total memory
        if (memory < _minMemoryConsumption)
            _minMemoryConsumption = memory;
        if (memory > _maxMemoryConsumption)
            _maxMemoryConsumption = memory;
        _totalMemoryConsumption += memory;
        _memoryMeasurementCount++;

        // Calculate average memory and elapsed time
        long avgMemory = _memoryMeasurementCount > 0 ? _totalMemoryConsumption / _memoryMeasurementCount : 0;
        TimeSpan elapsed = DateTimeOffset.Now - _operationStartTime;
        string elapsedStr = elapsed.TotalSeconds < 60
            ? $"{elapsed.TotalSeconds:F0}s"
            : $"{elapsed.TotalMinutes:F0}m";

        // Einfache Carriage Return Lösung auf zwei Zeilen
        Console.Write($"\r Mem: {memory}MB | Avg: {avgMemory}MB | Rows: {targetRowCount:N0} | Time: {elapsedStr}   ");

    }

    private static void PrintDiagnostics() {
        long memory = GC.GetTotalMemory(false) / 1024 / 1024;
        int targetRowCount = GetRowCount("TargetTable");
        long avgMemory = _memoryMeasurementCount > 0 ? _totalMemoryConsumption / _memoryMeasurementCount : 0;
        TimeSpan elapsed = DateTimeOffset.Now - _operationStartTime;

        Console.WriteLine();
        Console.WriteLine("=== Diagnostics ===");
        Console.WriteLine($"TargetTable Row Count: {targetRowCount:N0}");
        Console.WriteLine($"Current Memory: {memory:N0} MB");
        Console.WriteLine($"Min Memory: {_minMemoryConsumption:N0} MB");
        Console.WriteLine($"Avg Memory: {avgMemory:N0} MB");
        Console.WriteLine($"Max Memory: {_maxMemoryConsumption:N0} MB");
        if (_operationStartTime != DateTimeOffset.MinValue) {
            Console.WriteLine($"Elapsed Time: {elapsed.TotalSeconds:F2}s");
        }
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

    private static void ResetMemoryTracking() {
        _minMemoryConsumption = long.MaxValue;
        _maxMemoryConsumption = 0;
        _totalMemoryConsumption = 0;
        _memoryMeasurementCount = 0;
        _operationStartTime = DateTimeOffset.Now;
    }

    private static SqlConnectionManager GetSqlConnection() =>
        new(ConnectionString);
}
