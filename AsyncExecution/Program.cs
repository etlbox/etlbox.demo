using ETLBox;
using ETLBox.DataFlow;
using Serilog;
using Serilog.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace SUP94_AsyncExecution;
internal class Program {
    static async Task Main(string[] args) {

        //Settings.MaxBufferSize = 5000;
        if (File.Exists("log.txt"))
            File.Delete("log.txt");
        var log = new LoggerConfiguration()
            .WriteTo.File("log.txt")
            .CreateLogger();
        var ilogInstance = new SerilogLoggerFactory(log).CreateLogger("ETLBox");
        Settings.LogInstance = ilogInstance;

        MemorySource<MyRow> source = new();
        for (int i = 0; i < 4000; i++)
            source.DataAsList.Add(new MyRow() { Id = i, Value = "Test" + i });

        Multicast<MyRow> multicast = new Multicast<MyRow>();
        RowTransformation<MyRow> trans1 = new RowTransformation<MyRow>();
        trans1.OnCompletion = () => Console.WriteLine("Transformation **1** completed");
        trans1.TransformationFunc = row => {
            row.Value = row.Value + " - transformed";
            Task.Delay(1).GetAwaiter().GetResult();
            log.Information("Transformation **1**: " + row.Id + ":" + row.Value);
            return row;
        };

        RowTransformation<MyRow> trans2 = new RowTransformation<MyRow>();
        trans2.OnCompletion = () => Console.WriteLine("Transformation **2** completed");
        trans2.TransformationFunc = row => {
            row.Value = row.Value + " - transformed again";
            log.Information("Transformation **2**: " + row.Id + ":" + row.Value);
            return row;
        };

        CustomDestination<MyRow> dest1 = new CustomDestination<MyRow>();
        dest1.OnCompletion = () => Console.WriteLine("Destination **1** completed");
        dest1.WriteAction = (row, _) => {
            Task.Delay(1).GetAwaiter().GetResult();
            log.Information("Destination **1**: " + row.Id + ":" + row.Value);
        };

        CustomDestination<MyRow> dest2 = new CustomDestination<MyRow>();
        dest2.OnCompletion = () => Console.WriteLine("Destination **2** completed");
        dest2.WriteAction = (row, _) => {
            log.Information("Destination **2**: " + row.Id + ":" + row.Value);
        };

        source.LinkTo(multicast);
        multicast.LinkTo(trans1);
        multicast.LinkTo(trans2);
        trans1.LinkTo(dest1);
        trans2.LinkTo(dest2);

        //await Network.ExecuteAsync(source);
        Task t = Network.ExecuteAsync(source);
        while (t.Status != TaskStatus.RanToCompletion) {
            Console.WriteLine("Network still running - Waiting for completion...");
            await Task.Delay(1000);
        }
        await t;
    }
}

internal class MyRow {
    public int Id { get; set; }
    public string Value { get; set; }
}
