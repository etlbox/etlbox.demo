using ETLBoxDemo.DifferentDBs;
using System;
using System.Collections.Generic;

namespace ETLBoxDemo.DifferentSourcesDestinations
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Running import/export CSV...");
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
