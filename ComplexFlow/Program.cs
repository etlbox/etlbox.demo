using ALE.ETLBox;
using ALE.ETLBox.Logging;
using System;

namespace ALE.ComplexFlow {
    class Program {
        static void Main(string[] args) {

            var connString = new ConnectionString(
                @"Data Source=.;Initial Catalog=demo;Integrated Security=false;User=sa;password=reallyStrongPwd123");

            Console.WriteLine("Starting DataFlow example - preparing database");
            PrepareDb prep = new PrepareDb();
            prep.Prepare(connString);

            Console.WriteLine("Running data flow");
            StartLoadProcessTask.Start("Demo Process");
            DataFlowTasks dft = new DataFlowTasks();
            dft.Run();
            EndLoadProcessTask.End("Finishing demo");
            Console.WriteLine("Dafaflow finished...");

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}
