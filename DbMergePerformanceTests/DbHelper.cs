using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.SqlServer;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MergePerformanceIssue
{
    internal class DbHelper
    {


        public static bool CleanSourceTable = true;
        public static void CreateDatabaseIfNeeded(string dbName, SqlConnectionString connectionString) {
            var masterConnection = new SqlConnectionManager(connectionString.CloneWithMasterDbName());
            if (!IfDatabaseExistsTask.IsExisting(masterConnection, dbName))
                CreateDatabaseTask.Create(masterConnection, dbName);
        }

        public static void CreateTables(SqlConnectionManager connection) {

            TableDefinition SourceTableDef = new TableDefinition("source",
                new List<TableColumn>() {
                    new TableColumn("IdentityKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity: true),
                    new TableColumn("Id","BIGINT", allowNulls: false),
                    new TableColumn("LongValue1","BIGINT", allowNulls: false),
                    new TableColumn("LongValue2","BIGINT", allowNulls: false),
                    new TableColumn("LongValue3","BIGINT", allowNulls: false),
                    new TableColumn("LongValue4","BIGINT", allowNulls: false),
                    new TableColumn("LongValue5","BIGINT", allowNulls: false),
                    new TableColumn("LongValue6","BIGINT", allowNulls: false),
                    new TableColumn("LongValue7","BIGINT", allowNulls: false),
                    new TableColumn("LongValue8","BIGINT", allowNulls: false),
                    new TableColumn("LongValue9","BIGINT", allowNulls: false),
                    new TableColumn("LongValue10","BIGINT", allowNulls: false),
                    new TableColumn("StringValue1","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue2","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue3","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue4","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue5","VARCHAR(5)", allowNulls: false),
            });

            TableDefinition DestinationTableDef = new TableDefinition("destination",
               new List<TableColumn>() {
                    new TableColumn("IdentityKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity: true),
                    new TableColumn("Id","BIGINT", allowNulls: false),
                    new TableColumn("LongValue1","BIGINT", allowNulls: false),
                    new TableColumn("LongValue2","BIGINT", allowNulls: false),
                    new TableColumn("LongValue3","BIGINT", allowNulls: false),
                    new TableColumn("LongValue4","BIGINT", allowNulls: false),
                    new TableColumn("LongValue5","BIGINT", allowNulls: false),
                    new TableColumn("LongValue6","BIGINT", allowNulls: false),
                    new TableColumn("LongValue7","BIGINT", allowNulls: false),
                    new TableColumn("LongValue8","BIGINT", allowNulls: false),
                    new TableColumn("LongValue9","BIGINT", allowNulls: false),
                    new TableColumn("LongValue10","BIGINT", allowNulls: false),
                    new TableColumn("StringValue1","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue2","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue3","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue4","VARCHAR(5)", allowNulls: false),
                    new TableColumn("StringValue5","VARCHAR(5)", allowNulls: false),
           });


            TableDefinition DeltaTableDef = new TableDefinition("delta",
                new List<TableColumn>() {
                    new TableColumn("IdentityKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity: true),
                    new TableColumn("Id","BIGINT", allowNulls: false),
                    new TableColumn("ChangeDate","DATETIME2(7)", allowNulls: false),
                    new TableColumn("ChangeAction","VARCHAR(100)", allowNulls: false),
            });

            if (!IfTableOrViewExistsTask.IsExisting(connection, SourceTableDef.Name))
                CreateTableTask.Create(connection, SourceTableDef);
            else {
                if (CleanSourceTable) TruncateTableTask.Truncate(connection, SourceTableDef.Name);
            }
            if (!IfTableOrViewExistsTask.IsExisting(connection, DestinationTableDef.Name))
                CreateTableTask.Create(connection, DestinationTableDef);
            else
                TruncateTableTask.Truncate(connection, DestinationTableDef.Name);

            if (!IfTableOrViewExistsTask.IsExisting(connection, DeltaTableDef.Name))
                CreateTableTask.Create(connection, DeltaTableDef);
            else
                TruncateTableTask.Truncate(connection, DeltaTableDef.Name);
        }

        public static void InsertTestDataSource(SqlConnectionManager connection, int start, int end) {

            if (!CleanSourceTable && RowCountTask.Count(connection, "source") > 0) return;
            sourceStart = start;
            sourceEnd = end;
            var source = new MemorySource<MergeRow>();
            source.Data = ProduceSource();
            source.DisableLogging = true;
            var dest = new DbDestination<MergeRow>(connection, "source");
            dest.DisableLogging = true;

            source.LinkTo(dest);
            Network.Execute(source);

        }

        public static void InsertTestDataDestination(SqlConnectionManager connection, int start, int end) {

            destinationStart = start;
            destinationEnd = end;
            var source = new MemorySource<MergeRow>();
            source.Data = ProduceDest();
            source.DisableLogging = true;
            var dest = new DbDestination<MergeRow>(connection, "destination");
            dest.DisableLogging = true;

            source.LinkTo(dest);
            Network.Execute(source);

        }

        static int sourceStart = 50;
        static int sourceEnd = 100;
        static IEnumerable<MergeRow> ProduceSource() {
            while (sourceStart < sourceEnd) {
                yield return new MergeRow() {
                    Id = sourceStart,
                    LongValue1 = sourceStart % 5,
                    LongValue2 = sourceStart % 5,
                    LongValue3 = sourceStart % 5,
                    LongValue4 = sourceStart % 5,
                    LongValue5 = sourceStart % 5,
                    LongValue6 = sourceStart % 5,
                    LongValue7 = sourceStart % 5,
                    LongValue8 = sourceStart % 5,
                    LongValue9 = sourceStart % 5,
                    LongValue10 = sourceStart % 5,
                    StringValue1 = "ABCD" + (sourceStart % 5),
                    StringValue2 = "ABCD" + (sourceStart % 5),
                    StringValue3 = "ABCD" + (sourceStart % 5),
                    StringValue4 = "ABCD" + (sourceStart % 5),
                    StringValue5 = "ABCDE",
                };
                sourceStart++;
            }

        }

        static int destinationStart = 1;
        static int destinationEnd = 75;
        static IEnumerable<MergeRow> ProduceDest() {
            while (destinationStart < destinationEnd) {
                yield return new MergeRow() {
                    Id = destinationStart,
                    LongValue1 = destinationStart % 3,
                    LongValue2 = destinationStart % 3,
                    LongValue3 = destinationStart % 3,
                    LongValue4 = destinationStart % 3,
                    LongValue5 = destinationStart % 3,
                    LongValue6 = destinationStart % 5,
                    LongValue7 = destinationStart % 5,
                    LongValue8 = destinationStart % 5,
                    LongValue9 = destinationStart % 5,
                    LongValue10 = destinationStart % 5,
                    StringValue1 = "ABCD" + (destinationStart % 3),
                    StringValue2 = "ABCD" + (destinationStart % 3),
                    StringValue3 = "ABCD" + (destinationStart % 3),
                    StringValue4 = "ABCD" + (destinationStart % 5),
                    StringValue5 = "ABCDE",
                };
                destinationStart++;
            }

        }
    }
}

