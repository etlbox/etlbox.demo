using ETLBox.ControlFlow;
using ETLBox.SqlServer;
using System;
using System.Collections.Generic;

namespace ETLBoxDemo.StarSchema
{
    public static class PrepareDb
    {
        public static void RecreateDatabase(string dbName, SqlConnectionString connectionString)
        {
            var masterConnection = new SqlConnectionManager(connectionString.CloneWithMasterDbName());
            DropDatabaseTask.DropIfExists(masterConnection, dbName);
            CreateDatabaseTask.Create(masterConnection, dbName);
        }
        public static void CreateOLTPTables(SqlConnectionString connectionString)
        {
            var connMan = new SqlConnectionManager(connectionString);

            TableDefinition OrderDataTableDef = new TableDefinition("Orders",
                new List<TableColumn>() {
                    new TableColumn("OrderId", "INT",allowNulls: false, isPrimaryKey:true),
                    new TableColumn("OrderDate","DATETIME", allowNulls: false),
                    new TableColumn("ProductNumber","CHAR(7)", allowNulls: false),
                    new TableColumn("CustomerNumber","CHAR(6)", allowNulls: false),
                    new TableColumn("ActualPrice","MONEY", allowNulls: false),
            });

            TableDefinition CustomerTableDef = new TableDefinition("Customer",
                new List<TableColumn>() {
                    new TableColumn("CustomerNumber", "CHAR(6)",allowNulls: false, isPrimaryKey:true),
                    new TableColumn("Name","NVARCHAR(200)", allowNulls: false),
            });

            TableDefinition ProductTableDef = new TableDefinition("Product",
               new List<TableColumn>() {
                    new TableColumn("ProductNumber", "CHAR(7)",allowNulls: false, isPrimaryKey:true),
                    new TableColumn("Name", "NVARCHAR(50)",allowNulls: false),
                    new TableColumn("Description", "NVARCHAR(500)",allowNulls: true),
                    new TableColumn("RecommendedPrice","MONEY", allowNulls: false),
                    new TableColumn("LastUpdated","DATETIME", allowNulls: false)

            });

            //Create demo tables & fill with demo data
            OrderDataTableDef.CreateTable(connMan) ;
            CustomerTableDef.CreateTable(connMan);
            ProductTableDef.CreateTable(connMan);            
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Customer VALUES('C-1000', 'Kevin Doe')");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Customer VALUES('C-1001','Nick Newman')");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Customer VALUES('C-1002','Zoe Trunk')");

            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Product VALUES('P-00010','Smartphone', 'The newest phone',399, '2023-01-01')");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Product VALUES('P-00011','uPhone', 'Same as a smartphone, but double the price',799,'2023-01-01')");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Product VALUES('P-00012','Computer', 'A brand new desktop',899,'2023-01-01')");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Product VALUES('P-00013','Notebook', NULL,1599,'2023-01-01')");

            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10003,'2023-01-01', 'P-00010', 'C-1000', 379)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10007,'2023-01-02', 'P-00011', 'C-1000', 699)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10012,'2023-01-03', 'P-00012', 'C-1000', 849)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10016,'2023-01-01', 'P-00012', 'C-1001', 949)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10020,'2023-01-02', 'P-00011', 'C-1001', 849)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10033,'2023-01-03', 'P-00013', 'C-1001', 1699)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10053,'2023-01-01', 'P-00010', 'C-1002', 299)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10193,'2023-01-01', 'P-00011', 'C-1002', 699)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10253,'2023-01-03', 'P-00012', 'C-1002', 799)");
            SqlTask.ExecuteNonQuery(connMan, "INSERT INTO Orders VALUES(10323,'2023-01-03', 'P-00013', 'C-1002', 1299)");
            
        }

        public static void CreateStarSchema(SqlConnectionString connectionString) {
            var connMan = new SqlConnectionManager(connectionString);

            TableDefinition OrderFactTableDef = new TableDefinition("FactOrders",
                new List<TableColumn>() {
                    new TableColumn("FactId", "INT",allowNulls: false, isPrimaryKey:true, isIdentity:true),
                    new TableColumn("SourceOrderId", "INT",allowNulls: false),
                    new TableColumn("DateDim","INT", allowNulls: false),
                    new TableColumn("ProductDim","INT", allowNulls: false),
                    new TableColumn("CustomerDim","INT", allowNulls: false),                    
                    new TableColumn("ActualPriceFact","MONEY", allowNulls: false),
            });

            TableDefinition CustomerDimTableDef = new TableDefinition("DimCustomer",
                new List<TableColumn>() {
                    new TableColumn("DimId", "INT",allowNulls: false, isPrimaryKey:true, isIdentity: true),
                    new TableColumn("CustomerNumber", "NVARCHAR(10)",allowNulls: false),
                    new TableColumn("Name","NVARCHAR(200)", allowNulls: true),
            });

            TableDefinition ProductDimTableDef = new TableDefinition("DimProduct",
               new List<TableColumn>() {
                   new TableColumn("DimId", "INT",allowNulls: false, isPrimaryKey:true, isIdentity: true),
                    new TableColumn("ProductNumber", "NVARCHAR(10)",allowNulls: false),
                    new TableColumn("Name", "NVARCHAR(50)",allowNulls: true),
                    new TableColumn("Description", "NVARCHAR(500)",allowNulls: true),
                    new TableColumn("RecommendedPrice","MONEY", allowNulls: false),
                    new TableColumn("ValidFrom","DATETIME", allowNulls: false, isPrimaryKey:true),
                    new TableColumn("ValidTo","DATETIME", allowNulls: false),
            });

            //Create demo tables & fill with demo data
            OrderFactTableDef.CreateTable(connMan);
            CustomerDimTableDef.CreateTable(connMan);
            ProductDimTableDef.CreateTable(connMan);          

        }
    }
}
