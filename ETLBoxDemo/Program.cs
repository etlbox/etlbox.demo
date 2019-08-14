using System;

namespace ALE.ETLBoxDemo {
    class Program {
        static void Main(string[] args) {

            Console.WriteLine("Starting ControlFlow example");
            ControlFlowTasks cft = new ControlFlowTasks();
            cft.Start();
            Console.WriteLine("ControlFlow finished...");

            Console.WriteLine("Start Logging example");
            Logging log = new Logging();
            log.Start();
            Console.WriteLine("Logging finished...");

            Console.WriteLine("Starting DataFlow example");
            DataFlowTasks dft = new DataFlowTasks();
            dft.Preparation();
            dft.Start();
            Console.WriteLine("Dafaflow finished...");

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}
