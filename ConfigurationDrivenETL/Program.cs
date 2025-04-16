using ConfigurationDrivenETL;
using ETLBox;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Json;
using ETLBox.Xml;
using Newtonsoft.Json;
using System.Dynamic;

List<string> inputFilesCsv = Directory.GetFiles("InputData/Csv", "*.csv").ToList();
List<string> inputFilesJson = Directory.GetFiles("InputData/Json", "*.json").ToList();
List<string> inputFilesXml = Directory.GetFiles("InputData/Xml", "*.xml").ToList();

string configPathCsv = "Configuration/CsvDefaultMappings.json";
string configPathJson = "Configuration/JsonDefaultMappings.json";
string configPathXml = "Configuration/XmlDefaultMappings.json";

//string rowMapPath = "user_adjustments.json"; // optional, dummy logic
string outputFile = "merged_output.csv";

//Path for Csv
var csvSource = new CsvSource();
csvSource.HasNextUri = (_) => inputFilesCsv.Count > 0;
csvSource.GetNextUri = (_) => GetNextInputFile(inputFilesCsv);
var csvColumnTrans = CreateColumnTransformation(configPathCsv);

//Path for Json
var jsonSource = new JsonSource();
jsonSource.HasNextUri = (_) => inputFilesJson.Count > 0;
jsonSource.GetNextUri = (_) => GetNextInputFile(inputFilesJson);
var jsonColumnTrans = CreateColumnTransformationWithPath(configPathJson);


//Path for Xml
var xmlSource = new XmlSource();
xmlSource.ElementName = "Account";
xmlSource.HasNextUri = (_) => inputFilesXml.Count > 0;
xmlSource.GetNextUri = (_) => GetNextInputFile(inputFilesXml);
var xmlColumnTrans = CreateColumnTransformationWithPath(configPathXml);



var toAccountTransformation = ConvertToAccountTransformation();
var outputFileDestination = new CsvDestination<AccountRecord>(outputFile); ;

csvSource.LinkTo(csvColumnTrans);
csvColumnTrans.LinkTo(toAccountTransformation);

jsonSource.LinkTo(jsonColumnTrans);
jsonColumnTrans.LinkTo(toAccountTransformation);

xmlSource.LinkTo(xmlColumnTrans);
xmlColumnTrans.LinkTo(toAccountTransformation);

toAccountTransformation.LinkTo(outputFileDestination);


var errorDest = new JsonDestination<ETLBoxError>("errors.json");
Network.Init(toAccountTransformation)
    .LinkAllErrorTo(errorDest)
    .Execute();


// Print results
Console.WriteLine("Received data:");
foreach (var line in File.ReadLines(outputFile)) {
    Console.WriteLine(line);
}

Console.WriteLine("-------------------");
Console.WriteLine("Error log:");
Console.WriteLine(File.ReadAllText("errors.json"));

ColumnTransformation CreateColumnTransformation(string configPath) {
    var config = JsonConvert.DeserializeObject<MappingConfig>(File.ReadAllText(configPath));
    var colTrans = new ColumnTransformation();
    colTrans.RenameColumns = config.ColumnMappings
        .Select(m => new RenameColumn { CurrentName = m.source, NewName = m.destination })
        .ToList();
    return colTrans;
}

RowTransformation<ExpandoObject, ExpandoObject> CreateColumnTransformationWithPath(string configPath) {
    var config = JsonConvert.DeserializeObject<MappingConfig>(File.ReadAllText(configPath));
    var rowTrans = new RowTransformation<ExpandoObject, ExpandoObject>();

    rowTrans.TransformationFunc = row => {
        var dict = (IDictionary<string, object>)row;
        foreach (var mapping in config.ColumnMappings) {
            var value = GetValueByPath(dict, mapping.source);
            if (value != null)
                dict[mapping.destination] = value;
        }
        return row;
    };

    return rowTrans;

    object GetValueByPath(IDictionary<string, object> dict, string path) {
        var parts = path.Split('.');
        object current = dict;

        foreach (var part in parts) {
            if (current is IDictionary<string, object> currentDict && currentDict.TryGetValue(part, out var next)) {
                current = next;
            } else {
                return null;
            }
        }

        return current;
    }
}

RowTransformation<ExpandoObject, AccountRecord> ConvertToAccountTransformation() => new RowTransformation<ExpandoObject, AccountRecord>(row => {
    IDictionary<string,object> dict  = row as IDictionary<string,object>;
    var result = new AccountRecord();
    if (dict.ContainsKey("AccountNumber"))
        result.AccountNumber = dict["AccountNumber"]?.ToString();
    if (dict.ContainsKey("Name") && dict["Name"] != null)
        result.Name = dict["Name"]?.ToString();
    else
        throw new Exception("Name is missing");
    if (dict.ContainsKey("Balance"))
        result.Balance = decimal.Parse(dict["Balance"]?.ToString() ?? "0");
    if (dict.ContainsKey("Currency"))
        result.Currency = dict["Currency"]?.ToString();
    return result;
});

static string GetNextInputFile(List<string> inputFiles) {
    var next = inputFiles.First();
    inputFiles.RemoveAt(0);
    return next;
}