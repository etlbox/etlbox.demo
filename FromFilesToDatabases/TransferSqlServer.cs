using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System.Collections.Generic;

namespace ETLBoxDemo.DifferentDBs
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
        public string PostgresConnectionString = @"Server=localhost;Database=demo;User Id=postgres;Password=etlboxpassword;";

        public string SqlServerConnectionString = @"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=demo;";

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
                });
            DbDestination<NameListElement> dest = new DbDestination<NameListElement>(sqlConMan, "FullNameTable");
            source.LinkTo(trans);
            trans.LinkTo(dest);

            source.Execute();
            dest.Wait();
        }
    }
}
