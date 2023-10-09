using DataValidationPart1;
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
    });
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO VendorMaster (VendorName, Code, Custom, Country, Contact, Info)
        VALUES('BIG HOLDING', 'H1234', 'HD', 'US', 'Hans', 'T0')");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO VendorMaster (VendorName, Code, Custom,Country) 
        VALUES('UNICORN', 'UNI10', 'U', 'NO')");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO VendorMaster (VendorName, Code, Custom,Country) 
        VALUES('UNICORN TWO', 'UNI20', 'U', 'SE')");


//Create the data flow components
var source = new CsvSource<VendorMaster>("sourceData.csv");
var normalize = new RowTransformation<VendorMaster>(row => row.Normalize());
var errorTarget = new JsonDestination<VendorMaster>("errors.json");
var dbTarget = new DbDestination<VendorMaster>(connectionManager, "VendorMaster");
var duplicateCheck = new Distinct<VendorMaster>();
var lookupExisting = new LookupTransformation<VendorMaster, VendorMasterDbEntry>();
var lookupSource = new DbSource<VendorMasterDbEntry>(connectionManager, "VendorMaster");
var addNotDistinctErrorMessage = new RowTransformation<VendorMaster>(
    row => {
        row.ErrorMessage = "Duplicate in source data detected!";
        return row;
    });

lookupExisting.Source = lookupSource;

/* Linking the components */
source.LinkTo(normalize);

normalize.LinkTo(lookupExisting, row => row.IsValid());
normalize.LinkTo(errorTarget, row => true); //Valid rows are already send to the lookup!

lookupExisting.LinkTo(duplicateCheck, row => !row.IsInDb);
lookupExisting.LinkTo(errorTarget, row => {
    row.ErrorMessage = "Record already exists in target database!";
    return true;
});

duplicateCheck.LinkTo(dbTarget);
duplicateCheck.LinkDuplicatesTo(addNotDistinctErrorMessage);
addNotDistinctErrorMessage.LinkTo(errorTarget);

//Execute the network
await Network.ExecuteAsync(source);

Console.WriteLine("Done!");

