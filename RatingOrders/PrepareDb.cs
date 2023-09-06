using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using System;
using System.Collections.Generic;

namespace ETLBoxDemo.RatingOrdersExample
{
    public static class PrepareDb
    {
        public static void RecreateDatabase(string dbName, SqlConnectionString connectionString)
        {
            var masterConnection = new SqlConnectionManager(connectionString.CloneWithMasterDbName());
            DropDatabaseTask.DropIfExists(masterConnection, dbName);
            CreateDatabaseTask.Create(masterConnection, dbName);
        }
        public static void Prepare()
        {
            Console.WriteLine("Starting DataFlow example - preparing database");

            TableDefinition OrderDataTableDef = new TableDefinition("orders",
                new List<TableColumn>() {
                    new TableColumn("Key", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
                    new TableColumn("Number","nvarchar(100)", allowNulls: false),
                    new TableColumn("Item","nvarchar(200)", allowNulls: false),
                    new TableColumn("Amount","money", allowNulls: false),
                    new TableColumn("CustomerKey","int", allowNulls: false)
            });

            TableDefinition CustomerTableDef = new TableDefinition("customer",
                new List<TableColumn>() {
                    new TableColumn("Key", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
                    new TableColumn("Name","nvarchar(200)", allowNulls: false),
            });

            TableDefinition CustomerRatingTableDef = new TableDefinition("customer_rating",
               new List<TableColumn>() {
                    new TableColumn("Key", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
                    new TableColumn("CustomerKey", "int",allowNulls: false),
                    new TableColumn("TotalAmount","decimal(10,2)", allowNulls: false),
                    new TableColumn("Rating","nvarchar(3)", allowNulls: false)
            });

            //Create demo tables & fill with demo data
            OrderDataTableDef.CreateTable();
            CustomerTableDef.CreateTable();
            CustomerRatingTableDef.CreateTable();
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO customer values('Sandra Kettler')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO customer values('Nick Thiemann')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO customer values('Zoe Rehbein')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO customer values('Margit Gries')");
        }
    }
}
