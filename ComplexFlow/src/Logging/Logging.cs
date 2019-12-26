using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ALE.ETLBoxDemo {
    public class Logging {
        public void Start() {
            //Recreate database
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(@"Data Source=.\SQLEXPRESS;Integrated Security=SSPI;"));
            DropDatabaseTask.Drop("DemoDB");
            CreateDatabaseTask.Create("DemoDB");

            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(@"Data Source=.\SQLEXPRESS;Integrated Security=SSPI;Initial Catalog=DemoDB;"));

            //Logging (only works with existing configuration nlog config in App.config)

            CreateLogTablesTask.CreateLog();
            StartLoadProcessTask.Start("Process 1");
            ControlFlow.STAGE = "Staging";
            SqlTask.ExecuteNonQuery("some sql", "Select 1 as test");
            TransferCompletedForLoadProcessTask.Complete();
            ControlFlow.STAGE = "DataVault";

            Sequence.Execute("some custom code", () => { });
            LogTask.Warn("Some warning!");
            EndLoadProcessTask.End("Everything successful");

            string jsonLP = GetLoadProcessAsJSONTask.GetJSON();
            string jsonLog = GetLogAsJSONTask.GetJSON(1);
        }
    }
}
