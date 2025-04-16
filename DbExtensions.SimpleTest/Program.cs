using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DbExtensions;
using ETLBox.SqlServer;
using Microsoft.Data.SqlClient;

string connectionString = @"Data Source=np:\\.\pipe\LOCALDB#4BDFE241\tsql\query;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;TrustServerCertificate=true;";
var connection = new SqlConnection(connectionString);

var tableDefinition = TableDefinition.FromCLRType(ConnectionType.SqlServer, typeof(Customer));
CreateTableTask.CreateIfNotExists(new SqlConnectionManager(connectionString), tableDefinition);
    
var customers = Enumerable.Range(1, 4_999)
    .Select(i => new Customer { Id = i, Name = $"Customer {i}", City = $"City {i % 50}" });

connection.BulkInsert(customers);

public class Customer {
    public int Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }
}