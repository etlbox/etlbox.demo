using System;
using System.Collections.Generic;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;

namespace ETLBoxDemo.BasicExample
{
    class Program
    {
        static void Main(string[] args)
        {
            //Set up the connection manager to master
            var masterConnection = new SqlConnectionManager("Data Source=10.211.55.2;User Id=sa;Password=YourStrong@Passw0rd;");
            //Recreate database
            DropDatabaseTask.DropIfExists(masterConnection, "demo");
            CreateDatabaseTask.Create(masterConnection, "demo");

            //Get connection manager to previously create database
            var dbConnection = new SqlConnectionManager("Data Source=10.211.55.2;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;");

            //Create destination table
            CreateTableTask.Create(dbConnection, "Table1", new List<TableColumn>()
            {
                new TableColumn("ID","int",allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Col1","nvarchar(100)",allowNulls:true),
                new TableColumn("Col2","smallint",allowNulls:true)
            });

            //Create dataflow for loading data from csv into table
            CsvSource<string[]> source = new CsvSource<string[]>("input.csv");
            RowTransformation<string[], MyData> row = new RowTransformation<string[], MyData>(
                input =>
                    new MyData() { Col1 = input[0], Col2 = input[1] }
            );
            DbDestination<MyData> dest = new DbDestination<MyData>(dbConnection, "Table1");

            //Link components & run data flow
            source.LinkTo(row);
            row.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Check if data exists in destination
            SqlTask.ExecuteReader(dbConnection, "Read all data from table1",
            "select Col1, Col2 from Table1",
                col1 => Console.WriteLine(col1.ToString() + ","),
                col2 => Console.WriteLine(col2.ToString()));

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}