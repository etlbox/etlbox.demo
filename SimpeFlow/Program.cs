using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;

namespace ALE.SimpeFlow
{
    class Program
    {
        static void Preparation()
        {
            SqlConnectionManager masterConnection = new SqlConnectionManager("Data Source=.;Integrated Security=false;User=sa;password=reallyStrongPwd123");
            DropDatabaseTask.DropIfExists(masterConnection, "demo");
            CreateDatabaseTask.Create(masterConnection, "demo");
            SqlConnectionManager dbConnection = new SqlConnectionManager("Data Source=.;Initial Catalog=demo;Integrated Security=false;User=sa;password=reallyStrongPwd123");

            CreateTableTask.Create(dbConnection, "OrderTable", new List<TableColumn>()
            {
                new TableColumn("Id", "INT"),
                new TableColumn("Item", "NVARCHAR(200)"),
                new TableColumn("Quantity", "INT"),
                new TableColumn("Price", "MONEY")
            });
        }

        static void Main(string[] args)
        {
            Preparation();

            SqlConnectionManager connMan = new SqlConnectionManager("Data Source=.;Initial Catalog=demo;Integrated Security=false;User=sa;password=reallyStrongPwd123");

            CsvSource<string[]> source = new CsvSource<string[]>("demodata.csv");

            RowTransformation<string[], Order> rowTrans = new RowTransformation<string[], Order>(
              row => new Order()
              {
                  Id = int.Parse(row[0]),
                  Item = row[1],
                  Quantity = int.Parse(row[2]) + int.Parse(row[3]),
                  Price = double.Parse(row[4]) * 100
              });

            DbDestination<Order> dest = new DbDestination<Order>(connMan, "OrderTable");

            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            source.Execute();

            dest.Wait();

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}
