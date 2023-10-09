using DataValidationPart1;
using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Json;
using ETLBox.SqlServer;
using Microsoft.Identity.Client;
using System.Net.WebSockets;

//Adjust connection string to your local database
string SqlConnectionString = @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true";

//Prepare the target database
SqlConnectionManager connectionManager = new SqlConnectionManager(SqlConnectionString);
DropTableTask.DropIfExists(connectionManager, "VendorMaster");
CreateTableTask.CreateIfNotExists(connectionManager, "VendorMaster",
    new List<TableColumn>() {
        new TableColumn() { Name = "Id", DataType = "INT", AllowNulls = false, IsPrimaryKey = true, IsIdentity = true },
        new TableColumn() { Name = "VendorName", DataType = "VARCHAR(100)", AllowNulls = false },
        new TableColumn() { Name = "Code", DataType = "CHAR(5)", AllowNulls = false },
        new TableColumn() { Name = "Custom", DataType = "VARCHAR(10)", AllowNulls = false },
        new TableColumn() { Name = "Country", DataType = "CHAR(2)", AllowNulls = true },
        new TableColumn() { Name = "Contact", DataType = "VARCHAR(50)", AllowNulls = true },
        new TableColumn() { Name = "Info", DataType = "VARCHAR(50)", AllowNulls = true },
        new TableColumn() { Name = "ValidFrom", DataType = "DATETIME", AllowNulls = false },
        new TableColumn() { Name = "ValidTo", DataType = "DATETIME", AllowNulls = false },
    });
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
var source = new CsvSource<VendorMaster>("sourceData.csv");
var normalize = new RowTransformation<VendorMaster>(row => row.Normalize());
var errorTarget = new JsonDestination<VendorMaster>("errors.json");
var dbTarget = new DbDestination<VendorMaster>(connectionManager, "VendorMaster");
var duplicateCheck = new Distinct<VendorMaster>();
var lookupExisting = new LookupTransformation<VendorMaster, VendorMasterDbEntry>();
var lookupSource = new DbSource<VendorMasterDbEntry>(connectionManager, "VendorMaster");
lookupExisting.Source = lookupSource;

//Adjust ValidFrom to current data if record exists
var adjustValidFrom = new RowTransformation<VendorMaster>(row => {
    if (row.IsInDb)
        row.ValidFrom = DateTime.Now;
    return row;
});

//Add custom error message to duplicates
var addNotDistinctErrorMessage = new RowTransformation<VendorMaster>(
    row => {
        row.ErrorMessage = "Duplicate in source data detected!";
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

normalize.LinkTo(lookupExisting, row => row.IsValid());
normalize.LinkTo(errorTarget, row => true); //Valid rows are already send to the lookup!

lookupExisting.LinkTo(adjustValidFrom);
adjustValidFrom.LinkTo(duplicateCheck);

duplicateCheck.LinkTo(dbTarget);
duplicateCheck.LinkDuplicatesTo(addNotDistinctErrorMessage);
addNotDistinctErrorMessage.LinkTo(errorTarget);

//Execute the network
await Network.ExecuteAsync(source);

Console.WriteLine("Done!");

