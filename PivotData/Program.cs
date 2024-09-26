using ETLBox;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Helper;
using System.Dynamic;
using System.Globalization;

CsvSource source = new CsvSource("input.csv");
CsvDestination destination = new CsvDestination("output.csv");

Aggregation aggregation = new Aggregation();
aggregation.AggregateColumns = new[] {
    new AggregateColumn() { InputValuePropName = "Sales", AggregationMethod = AggregationMethod.Sum, AggregatedValuePropName = "Sales" },
};
aggregation.GroupColumns = new[] {
    new GroupColumn() { GroupPropNameInInput= "Category", GroupPropNameInOutput = "Category" },
    new GroupColumn() { GroupPropNameInInput= "Month", GroupPropNameInOutput = "Month" }
};

BlockTransformation block = new BlockTransformation();
block.BlockTransformationFunc = allRows => {
    Dictionary<string,ExpandoObject> csvOutput = new Dictionary<string, ExpandoObject>();
    foreach (IDictionary<string,object> row in allRows) {
        string category = row["Category"] as string;
        IDictionary<string,object> outputRow;
        if (csvOutput.ContainsKey(category))
            outputRow = csvOutput[category];
        else {
            outputRow = new ExpandoObject();
            outputRow["Category"] = category;
            csvOutput.Add(category, outputRow as ExpandoObject);
        }

        outputRow.Add(row["Month"] as string, row["Sales"]);
    }

    return csvOutput.Values.ToArray();
};

source.LinkTo(aggregation);
aggregation.LinkTo(block);
block.LinkTo(destination);

Network.Execute(source);

Console.WriteLine(File.ReadAllText("output.csv"));  

//Output
/*
Category,Jan,Feb
A,150,150
B,200
*/