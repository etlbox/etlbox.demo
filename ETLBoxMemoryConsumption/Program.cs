using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading;
using System.Threading.Tasks;
using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.SqlServer;

namespace ETLBoxMemoryConsumption;

public static class Program
{
    static string DatabaseName => "demo";
    public static async Task Main() {
        string input;
        do {
            Console.WriteLine("create | load (100k) | sync | read | gc | stop");
            input = Console.ReadLine();

            switch (input) {
                case ("create"):
                    CreateSourceDestinationTables();
                    Console.WriteLine($"Source and Target tables successfully (re)created.");
                    PrintDiagnostics();
                    break;
                case ("load"):
                    await InsertTestDataSource(1, 100_000);
                    Console.WriteLine($"Loading done.");
                    PrintDiagnostics();
                    break;
                case ("sync"):
                    await FullLoadAsync().ConfigureAwait(false);
                    Console.WriteLine($"Sync done.");
                    PrintDiagnostics();
                    break;
                case ("gc"):
                    GC.Collect();
                    Console.WriteLine("GC called.");
                    PrintDiagnostics();
                    break;
                case ("read"):
                    LoadSourceIntoList();
                    PrintDiagnostics();
                    break;
                default:
                    break;
            }
        } while (input != "stop");
    }

    private static void PrintDiagnostics() {
        Console.WriteLine($"Current row counts: SourceTable - {GetRowCount("SourceTable")}, TargetTable - {GetRowCount("TargetTable")}");
        Console.WriteLine($"Current memory consumption (managed Heap): {GC.GetTotalMemory(true) / 1024} kilobytes.");
    }

    private static int GetRowCount(string tableName) {
        using var connection = new SqlConnectionManager($"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;");
        return RowCountTask.Count(connection, tableName);
    }

    private static void CreateSourceDestinationTables() {
        using var connection = new SqlConnectionManager($"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;");
        DropTableTask.DropIfExists(connection, "SourceTable");
        CreateTableTask.Create(connection, "SourceTable", new List<TableColumn>() {
            new TableColumn("ID", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
            new TableColumn("FirstName", "NVARCHAR(100)", allowNulls: true),
            new TableColumn("LastName", "NVARCHAR(100)", allowNulls: true)
        });
        DropTableTask.DropIfExists(connection, "TargetTable");
        CreateTableTask.Create(connection, "TargetTable", new List<TableColumn>() {
            new TableColumn("ID", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: false),
            new TableColumn("FirstName", "NVARCHAR(100)", allowNulls: true),
            new TableColumn("LastName", "NVARCHAR(100)", allowNulls: true)
        });

    }

    static int sourceStart;
    static int sourceEnd;
    public static async Task InsertTestDataSource(int start, int end) {
        using var connection = new SqlConnectionManager($"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;");
        sourceStart = start;
        sourceEnd = end;
        var source = new MemorySource();
        source.Data = ProduceSource();
        var dest = new DbDestination(connection, "SourceTable");

        source.LinkTo(dest);
        await Network.ExecuteAsync(source);

        static IEnumerable<ExpandoObject> ProduceSource() {
            while (sourceStart < sourceEnd) {
                dynamic row = new ExpandoObject();
                row.FirstName = "Vorname" + sourceStart % 5;
                row.LastName = "Nachname" + sourceStart % 5;
                yield return row;
                sourceStart++;
            }

        }
    }

    private static async Task FullLoadAsync()
    {
        using var source = new SqlConnectionManager($"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;");
        using var target = new SqlConnectionManager($"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;");
        await GetExpandoFullMergeNetwork(source, target)
            .ExecuteAsync(CancellationToken.None);

        Network GetExpandoFullMergeNetwork(SqlConnectionManager sourceConnectionManager, SqlConnectionManager targetConnectionManager) {
            var dbSource = new DbSource(sourceConnectionManager) {
                Sql = "SELECT [ID], [LastName], [FirstName] FROM [dbo].[SourceTable]",
            };
            var dbTarget = new DbMerge(targetConnectionManager, "TargetTable") {
                MergeMode = MergeMode.Full,
            };

            dbTarget.IdColumns = new List<IdColumn>();
            //foreach (var idColum in (List<string>)["ID"])
            dbTarget.IdColumns.Add(new() { IdPropertyName = "ID" });

            dbSource.LinkTo(dbTarget);
            return new Network(dbSource);
        }

    }

    private static void LoadSourceIntoList() {
        using var connection2 = new SqlConnectionManager($"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog={DatabaseName};TrustServerCertificate=true;");
        var source = new DbSource(connection2,"SourceTable");
        var dest = new CustomDestination();
        List<ExpandoObject> memTarget = new();
        dest.WriteAction = (row,_) => {
            memTarget.Add(row);
        };
        source.LinkTo(dest);
        Network.Execute(source,dest);
        Console.WriteLine($"Read {memTarget.Count} rows into memory list.");
        memTarget.Clear();
        memTarget = null;

    }
}
