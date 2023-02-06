using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Dynamic;

XmlSource source = new XmlSource("demo.xml", ResourceType.File);
source.XmlReaderSettings.DtdProcessing = System.Xml.DtdProcessing.Ignore;
source.ElementName = "entry";

RowTransformation row = new RowTransformation();
row.TransformationFunc = row => {
    dynamic newRow = new ExpandoObject();
    var json = JsonConvert.SerializeObject(row);
    var jobj = JObject.Parse(json);
    JToken nameToken = jobj.SelectToken("$.set.element[1].node.@name");
    JToken nodeToken = jobj.SelectToken("$.set.element[1].node.#text");
    newRow.Name = nameToken.Value<string>();
    newRow.NodeValue = nodeToken.Value<string>();
    return newRow;

};

MemoryDestination dest = new MemoryDestination();

source.LinkTo(row);
row.LinkTo(dest);

Network.Execute(source);

/* Display data */

foreach (dynamic record in dest.Data) 
    Console.WriteLine($"Name: {record.Name}, Value: {record.NodeValue}");
