using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.DifferentSourcesDestinations
{
    public class NameListElement
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    public class ImportExportCSV
    {
        public string PostgresConnectionString = @"Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;";

        public void Prepare()
        {
            PostgresConnectionManager conMan = new PostgresConnectionManager(PostgresConnectionString);
            List<TableColumn> tc = new List<TableColumn>()
            {
                new TableColumn("Id","INTEGER",allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("FirstName", "VARCHAR(500)", allowNulls: true),
                new TableColumn("LastName", "VARCHAR(500)", allowNulls: true),
            };
            CreateTableTask.Create(conMan, "NameTable", tc);
        }

        public void Run()
        {
            PostgresConnectionManager conMan = new PostgresConnectionManager(PostgresConnectionString);
            //Import CSV
            CsvSource sourceCSV = new CsvSource("NameList.csv");
            DbDestination importDest = new DbDestination(conMan, "NameTable");
            sourceCSV.LinkTo(importDest);
            sourceCSV.Execute();
            importDest.Wait();

            //Export again
            DbSource<NameListElement> sourceTable = new DbSource<NameListElement>(conMan, "NameTable");
            CsvDestination<NameListElement> destCSV = new CsvDestination<NameListElement>("Export.csv");
            destCSV.Configuration.Delimiter = ";";
            sourceTable.LinkTo(destCSV);
            sourceTable.Execute();
            destCSV.Wait();
        }
    }
}
