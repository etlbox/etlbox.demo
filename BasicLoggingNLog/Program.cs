using ETLBox;
using ETLBox.DataFlow;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(builder => {
    builder
        .AddFilter("Microsoft", Microsoft.Extensions.Logging.LogLevel.Warning)
        .AddFilter("System", Microsoft.Extensions.Logging.LogLevel.Warning)
        .SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace)
        .AddNLog("nlog.config");
});
ETLBox.Settings.LogInstance = loggerFactory.CreateLogger("Default");

Settings.LogInstance = loggerFactory.CreateLogger("etl");
Settings.LogThreshold = 20;
Settings.AdditionalScope = new Dictionary<string, object> {
    { "env", "test" }
};

var source = new MemorySource<Row>();
for (int i = 0; i < 100; i++)
    source.DataAsList.Add(new Row() { Id = i, Value = "Test" + i });
var trans = new RowTransformation<Row>();
trans.TransformationFunc = row => {
    if (row.Id == 60)
        Settings.LogInstance.LogInformation("Id 60 was processed");
    return row;
};
var dest = new TextDestination<Row>("output.txt");
dest.WriteHeaderFunc = () => "Id|Value";
dest.WriteLineFunc = row => $"{row.Id}|{row.Value}";

source.LinkTo(trans).LinkTo(dest);
await Network.ExecuteAsync(source);


public class Row {
    public int Id { get; set; }
    public string Value { get; set; }
}