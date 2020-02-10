using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.DifferentDBs
{

    public class NameListElement
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string FullName { get; set; }
    }

    public class TransferSqlServer
    {
        public string PostgresConnectionString = @"Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;";

        public string SqlServerConnectionString = @"Data Source=.;Initial Catalog=ETLBox_DataFlow;Integrated Security=false;User=sa;password=reallyStrongPwd123";

        public void Prepare()
        {
            SqlConnectionManager conMan = new SqlConnectionManager(SqlServerConnectionString);
            List<TableColumn> tc = new List<TableColumn>()
            {
                new TableColumn("Id","INTEGER",allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("FullName", "VARCHAR(1000)", allowNulls: true)
            };
            CreateTableTask.Create(conMan, "FullNameTable", tc);
        }

        public void Run()
        {
            PostgresConnectionManager postgresConMan = new PostgresConnectionManager(PostgresConnectionString);
            SqlConnectionManager sqlConMan = new SqlConnectionManager(SqlServerConnectionString);

            //Transfer across databases
            DbSource<NameListElement> source = new DbSource<NameListElement>(postgresConMan, "NameTable");
            RowTransformation<NameListElement> trans = new RowTransformation<NameListElement>(
                row =>
                {
                    row.FullName = row.LastName + "," + row.FirstName;
                    return row;
                }) ;
            DbDestination<NameListElement> dest = new DbDestination<NameListElement>(sqlConMan, "FullNameTable");
            source.LinkTo(trans);
            trans.LinkTo(dest);

            source.Execute();
            dest.Wait();
        }
    }
}
