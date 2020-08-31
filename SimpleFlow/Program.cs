using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;

namespace ETLBoxDemo.SimpeFlow
{
    class Program
    {
        static void Preparation()
        {
            //Recreate database if it doesn't exist
            SqlConnectionManager masterConnection = new SqlConnectionManager("Data Source=10.211.55.2;User Id=sa;Password=YourStrong@Passw0rd;");
            DropDatabaseTask.DropIfExists(masterConnection, "demo");
            CreateDatabaseTask.Create(masterConnection, "demo");
            SqlConnectionManager dbConnection = new SqlConnectionManager("Data Source=10.211.55.2;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;");

            //Create destination table
            CreateTableTask.Create(dbConnection, "OrderTable", new List<TableColumn>()
            {
                new TableColumn("Id", "INT", allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Item", "NVARCHAR(50)"),
                new TableColumn("Quantity", "INT"),
                new TableColumn("Price", "DECIMAL(10,2)")
            });
        }

        static void Main(string[] args)
        {
            //Set up database
            Preparation();

            //Define the source
            CsvSource<string[]> source = new CsvSource<string[]>("demodata.csv");

            //Define the transformation
            RowTransformation<string[], Order> rowTrans = new RowTransformation<string[], Order>(
              row => new Order()
              {
                  Item = row[1],
                  Quantity = int.Parse(row[2]) + int.Parse(row[3]),
                  Price = int.Parse(row[4]) / 100
              });

            //DbDestination needs a connection manager pointing to the right database
            SqlConnectionManager connMan = new SqlConnectionManager("Data Source=10.211.55.2;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;");
            //Define the destination
            DbDestination<Order> dest = new DbDestination<Order>(connMan, "OrderTable");

            //Link & run flow
            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}
