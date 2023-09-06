using DataValidationExample;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Json;
using ETLBox.SqlServer;

//Adjust connection string to your local database
string SqlConnectionString = @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true";

//Prepare the target database
Console.WriteLine("Preparing example database target table.");
SqlConnectionManager connectionManager = new SqlConnectionManager(SqlConnectionString);
DropTableTask.DropIfExists(connectionManager, "dbtable");
CreateTableTask.CreateIfNotExists(connectionManager, "dbTable",
    new List<TableColumn>() {
        new TableColumn() { Name = "Id", DataType = "INT", AllowNulls = false, IsIdentity=true },
        new TableColumn() { Name = "CustomerName", DataType = "VARCHAR(100)", AllowNulls = false },
        new TableColumn() { Name = "Code1", DataType = "VARCHAR(100)", AllowNulls = false },
        new TableColumn() { Name = "Code2", DataType = "VARCHAR(100)", AllowNulls = false },
        new TableColumn() { Name = "Country", DataType = "VARCHAR(100)", AllowNulls = true }        
    });
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO dbTable (CustomerName, Code1, Code2, Country) 
      VALUES('Arthur', 'CodeA', 'PAN_A', 'IE')");
SqlTask.ExecuteNonQuery(connectionManager,
    @"INSERT INTO dbTable (CustomerName, Code1, Code2, Country) 
      VALUES('Bethanie', 'CodeB', 'PAN_B', 'NO')");


//Create the data flow components
var source = new CsvSource<CustomerRow>("source.csv");
var errorTarget = new JsonDestination<CustomerRow>("errors.json");
var dbTarget = new DbDestination<CustomerRow>(connectionManager, "dbtable");
var duplicateCheck = new Distinct<CustomerRow>();
var lookupExisting = new LookupTransformation<CustomerRow, CustomerDbEntry>();
var lookupSource = new DbSource<CustomerDbEntry>(connectionManager, "dbtable");
lookupExisting.Source = lookupSource;

/* Linking the components */
source.LinkTo(lookupExisting, row => row.IsValid());
source.LinkTo(errorTarget, row => {
    bool isInvalid = !row.IsValid();
    row.ErrorMessage = isInvalid ? "Validation for row failed!" : row.ErrorMessage;
    return isInvalid;
});

lookupExisting.LinkTo(duplicateCheck, row => !row.IsInDb);
lookupExisting.LinkTo(errorTarget, row => {
    if (row.IsInDb)
        row.ErrorMessage = "Row with same combination of Code1/Code2 already exists in database!";
    return row.IsInDb;
});

duplicateCheck.LinkTo(dbTarget);
duplicateCheck.LinkDuplicatesTo(errorTarget, row => {
    row.ErrorMessage = "Row is a duplicate!";
    return true;
});

//Execute the network
Console.WriteLine("Starting the data flow.");
Network.Execute(source);

Console.WriteLine("Done!");