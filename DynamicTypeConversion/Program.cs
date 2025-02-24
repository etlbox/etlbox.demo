// Load configuration
using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Postgres;
using Newtonsoft.Json;
using System.Dynamic;

//Create demo table
PostgresConnectionManager dbConnection = new PostgresConnectionManager("Server=localhost;Database=demo;User Id=postgres;Password=etlboxpassword;");
Settings.DefaultDbConnection = dbConnection;
SqlTask.ExecuteNonQuery(@"DROP TABLE IF EXISTS demotable");
SqlTask.ExecuteNonQuery(@"CREATE TABLE demotable (
  ""Value1"" INT NULL,
  ""Value2"" VARCHAR(100) NULL
 )");
SqlTask.ExecuteNonQuery(@"INSERT INTO demotable (""Value1"", ""Value2"") VALUES (1, 'FirstTest'), (2, 'SecondTest'), (3, 'ThirdTest')");
SqlTask.ExecuteNonQuery(@"DROP TABLE IF EXISTS destinationtable");
SqlTask.ExecuteNonQuery(@"CREATE TABLE destinationtable (
  ""Dest1"" VARCHAR(100),
  ""Dest2"" VARCHAR(1) NULL
 )");
string configJson = File.ReadAllText("config.json");
dynamic config = JsonConvert.DeserializeObject<ExpandoObject>(configJson);

// Read from a generic database source
DbSource source = new DbSource("demotable");

RowTransformation trans = new RowTransformation(row => {
    IDictionary<string, object> c = row as IDictionary<string,object>;
    IDictionary<string, object> result = new ExpandoObject();

    foreach (var configEntry in config.destinationColumn) {
        string sourceName = configEntry.sourceName;
        string destName = configEntry.name;

        if (c.ContainsKey(sourceName)) {
            object value = c[sourceName];
            // Apply optional transformations
            if (configEntry.convert == true)
                value = value.ToString();
            if (configEntry.trim == true && value is string) {
                value = (value as string).Trim().Substring(0,1);
            }

            result[destName] = value;
        }
    }
    return result as ExpandoObject;
});

DbDestination dest = new DbDestination("destinationtable");

source.LinkTo(trans).LinkTo(dest);
Network.Execute(source);

/* Table Content*/
/*
| Dest1 | Dest2 |
|-------|-------|
| 1     | F     |
| 2     | S     |
| 3     | T     |
*/

