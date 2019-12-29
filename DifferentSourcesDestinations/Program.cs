using ALE.DifferentDBs;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;

namespace ALE.DifferentSourcesDestinations
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Running import/export CSV any key to continue...");
            ImportExportCSV iec = new ImportExportCSV();
            iec.Prepare();
            iec.Run();

            Console.WriteLine("Running transfer into Sql Server..");
            TransferSqlServer tss = new TransferSqlServer();
            tss.Prepare();
            tss.Run();

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}
