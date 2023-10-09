using DataValidationPart3;
using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Json;
using ETLBox.SqlServer;
using System.Dynamic;

//Adjust connection string to your local database
string SqlConnectionString = @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true";
SqlConnectionManager connectionManager = new SqlConnectionManager(SqlConnectionString);

//Prepare the "meta" table that contains our table definition and rules
DropTableTask.DropIfExists(connectionManager, "Meta");
CreateTableTask.CreateIfNotExists(connectionManager, "Meta",
    new List<TableColumn>() {
        new TableColumn() { Name = "ColumnName", DataType = "VARCHAR(1024)", AllowNulls = false  },
        new TableColumn() { Name = "DataType", DataType = "VARCHAR(20)", AllowNulls = false  },
        new TableColumn() { Name = "IsMandatory", DataType = "INT", AllowNulls = false },
        new TableColumn() { Name = "IsBusinessKey", DataType = "INT", AllowNulls = false, DefaultValue = "0"  },
        new TableColumn() { Name = "FileColumnName", DataType = "VARCHAR(1024)", AllowNulls = false  },
        new TableColumn() { Name = "MinFieldLength", DataType = "INT", AllowNulls = true },
        new TableColumn() { Name = "MaxFieldLength", DataType = "INT", AllowNulls = true },
        new TableColumn() { Name = "Trim", DataType = "INT", AllowNulls = false, DefaultValue = "0" },
        new TableColumn() { Name = "Uppercase", DataType = "INT", AllowNulls = false, DefaultValue = "0" },
    });

SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO Meta (ColumnName, DataType, IsMandatory, IsBusinessKey, FileColumnName, MinFieldLength, MaxFieldLength, Trim, Uppercase)
        VALUES('VendorName','VARCHAR(100)', 1, 0, 'Name', 5, 50, 1, 1)");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO Meta (ColumnName, DataType, IsMandatory, IsBusinessKey, FileColumnName, MinFieldLength, MaxFieldLength, Trim, Uppercase)
        VALUES('Code','CHAR(5)', 1, 1, 'Code', 5, 5, 0, 0)");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO Meta (ColumnName, DataType, IsMandatory, IsBusinessKey, FileColumnName, MinFieldLength, MaxFieldLength, Trim, Uppercase)
        VALUES('Custom','VARCHAR(10)', 1, 1, 'Custom', NULL, NULL, 0, 0)");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO Meta (ColumnName, DataType, IsMandatory, IsBusinessKey, FileColumnName, MinFieldLength, MaxFieldLength, Trim, Uppercase)
        VALUES('Country','CHAR(2)', 0, 0, 'Country', NULL, NULL, 0, 0)");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO Meta (ColumnName, DataType, IsMandatory, IsBusinessKey, FileColumnName, MinFieldLength, MaxFieldLength, Trim, Uppercase)
        VALUES('Contact','VARCHAR(50)', 0, 0, 'Contact', NULL, NULL, 0, 0)");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO Meta (ColumnName, DataType, IsMandatory, IsBusinessKey, FileColumnName, MinFieldLength, MaxFieldLength, Trim, Uppercase)
        VALUES('Info','VARCHAR(50)', 0, 0, 'TraceInfo', NULL, NULL, 0, 0)");

//Load the configuration into memory - we are using ETLBox for this
var configSource = new DbSource<Meta>(connectionManager, "Meta");
var configDest = new MemoryDestination<Meta>();
configSource.LinkTo(configDest);
Network.Execute(configSource);
var config = configDest.Data;

//Prepare the target database based on configuration

DropTableTask.DropIfExists(connectionManager, "VendorMaster");
var columns = new List<TableColumn>();
columns.Add(new TableColumn() { Name = "Id", DataType = "INT", AllowNulls = false, IsPrimaryKey = true, IsIdentity = true });
columns.AddRange(config.Select(c =>
    new TableColumn() { Name = c.ColumnName, DataType = c.DataType, AllowNulls = !c.IsMandatory })
);
columns.Add(new TableColumn() { Name = "ValidFrom", DataType = "DATETIME", AllowNulls = false });
columns.Add(new TableColumn() { Name = "ValidTo", DataType = "DATETIME", AllowNulls = false });
CreateTableTask.CreateIfNotExists(connectionManager, "VendorMaster", columns);

SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO VendorMaster (VendorName, Code, Custom, Country, Contact, Info, ValidFrom,ValidTo)
        VALUES('BIG HOLDING', 'H1234', 'HD', 'US', 'Hans', 'T0', '1900-1-1','9999-12-31')");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO VendorMaster (VendorName, Code, Custom,Country, ValidFrom,ValidTo)
        VALUES('UNICORN', 'UNI10', 'U', 'NO', '1900-1-1','9999-12-31')");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO VendorMaster (VendorName, Code, Custom,Country, ValidFrom,ValidTo)
        VALUES('UNICORN TWO', 'UNI20', 'U', 'SE', '1900-1-1','9999-12-31')");


//Create the data flow components
var source = new CsvSource("sourceData.csv");
var normalize = new RowTransformation(row => Normalize(row));
var errorTarget = new JsonDestination("errors.json");
var dbTarget = new DbDestination(connectionManager, "VendorMaster");
dbTarget.ColumnMapping = config
    .Where(c => c.ColumnName != c.FileColumnName)
    .Select(c => new DbColumnMap() { DbColumnName = c.ColumnName, PropertyName = c.FileColumnName })
    .ToList();
var lookupExisting = new LookupTransformation();
var lookupSource = new DbSource(connectionManager, "VendorMaster");
lookupExisting.Source = lookupSource;
lookupExisting.MatchColumns = config
    .Where(c => c.IsBusinessKey)
    .Select(c => new MatchColumn() { InputPropertyName = c.FileColumnName, LookupSourcePropertyName = c.ColumnName })
    .ToList();
lookupExisting.RetrieveColumns = new[] {
    new RetrieveColumn() { InputPropertyName = "DbId", LookupSourcePropertyName ="Id"}
};
var duplicateCheck = new Distinct();
duplicateCheck.DistinctColumns = config
    .Where(c => c.IsBusinessKey)
    .Select(c => new DistinctColumn() { DistinctPropertyName = c.FileColumnName })
    .ToList();

//Adjust ValidFrom to current data if record exists
var adjustValidFrom = new RowTransformation(row => {
    dynamic r = row as dynamic;
    if ((row as IDictionary<string,object>).ContainsKey("DbId") && r.DbId > 0)
        r.ValidFrom = DateTime.Now;
    else 
        r.ValidFrom = new DateTime(1900, 1, 1);
    r.ValidTo = new DateTime(9999, 12, 31);
    return row;
});

//Add custom error message to duplicates
var addNotDistinctErrorMessage = new RowTransformation(
    row => {
        (row as dynamic).ErrorMessage = "Duplicate in source data detected!";
        return row;
    });

//Modify Merge to work with SCD-2
dbTarget.AfterBatchWrite = batch => {
    SqlTask.ExecuteNonQuery(connectionManager, @"
UPDATE VendorMaster
SET VendorMaster.ValidTo = calc.validto
FROM (SELECT Id,
             Code,
             Custom, 
             ValidFrom,
             LEAD(ValidFrom)
                  OVER (
                      PARTITION BY Code,Custom
                      ORDER BY ValidFrom
                      ) validto
      FROM VendorMaster) calc
WHERE VendorMaster.Id = calc.Id
  AND calc.validto IS NOT NULL
");
};


/* Linking the components */
source.LinkTo(normalize);

normalize.LinkTo(lookupExisting, row => IsValid(row));
normalize.LinkTo(errorTarget, row => true); //Valid rows are already send to the lookup!

//var debug = new MemoryDestination()
lookupExisting.LinkTo(adjustValidFrom);
adjustValidFrom.LinkTo(duplicateCheck);

duplicateCheck.LinkTo(dbTarget);
duplicateCheck.LinkDuplicatesTo(addNotDistinctErrorMessage);
addNotDistinctErrorMessage.LinkTo(errorTarget);

//Execute the network
await Network.ExecuteAsync(source);

Console.WriteLine("Done!");

dynamic Normalize(IDictionary<string,object> row) {
    foreach (var configEntry in config) {
        if (configEntry.Uppercase == true)
            row[configEntry.FileColumnName] = (row[configEntry.FileColumnName] as string).ToUpper();
        if (configEntry.Trim == true)
            row[configEntry.FileColumnName] = (row[configEntry.FileColumnName] as string).Trim();
    }
    return row;
}
bool IsValid(IDictionary<string, object> row) {
    foreach (var configEntry in config) {
        if (configEntry.IsMandatory == true &&
            string.IsNullOrEmpty(row[configEntry.FileColumnName] as string)) {
            row.Add("ErrorMessage", "Empty required column detected!");
            return false;
        }
        if (configEntry.MinFieldLength > 0 &&
            (row[configEntry.FileColumnName] as string).Length < configEntry.MinFieldLength) {
            row.Add("ErrorMessage", "Unsupported length!");
            return false;
        }
        if (configEntry.MaxFieldLength > 0 &&
            (row[configEntry.FileColumnName] as string).Length > configEntry.MaxFieldLength) {
            row.Add("ErrorMessage", "Unsupported length!");
            return false;
        }
    }
    return true;
}
