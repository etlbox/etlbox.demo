using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ParallelExecution
{
    public class MyLinkingRow
    {
        public int Col1 { get; set; }
        public string Col2 { get; set; }
    }

    public class Program
    {
        public static async Task Main(string[] args) {
            Console.WriteLine(Environment.NewLine + "Example Part 1 -  async network execution using await");
            await AwaitingTwoNetworks();
            Console.WriteLine(Environment.NewLine + "Example Part 2 -  parallel execution");
            await AwaitingTwoNetworksInParallel();
            Console.WriteLine(Environment.NewLine + "Example Part 3 -  WaitAll for 2 networks");
            WaitAllForTwoNetworks();
        }

        static Network CreateNetwork(string targetFileName) {
            var source = new MemorySource<MyLinkingRow>();
            for (int i = 0; i <= 5; i++)
                source.DataAsList.Add(new MyLinkingRow() { Col1 = i, Col2 = $"Test{i}" });

            var row = new RowTransformation<MyLinkingRow>();
            row.TransformationFunc = row => { 
                Console.WriteLine($"Sending row {row.Col1}|{row.Col2} into {targetFileName}");
                Task.Delay(10).Wait();
                return row; };

            var dest = new XmlDestination<MyLinkingRow>(targetFileName, ResourceType.File);
            source.LinkTo(row).LinkTo(dest);

            return new Network(source);
        }

        static async Task AwaitingTwoNetworks() {
            Console.WriteLine("Creating Network 1 & 2");
            var network1 = CreateNetwork("output1.xml");
            var network2 = CreateNetwork("output2.xml");

            Console.WriteLine("Awaiting Network 1");
            await network1.ExecuteAsync();
            Console.WriteLine("Network 1 complete - now awaiting network 2");
            await network2.ExecuteAsync();
            Console.WriteLine("Network 2 complete");
        }

        public static async Task AwaitingTwoNetworksInParallel() {
            Console.WriteLine("Creating Network 1 & 2");
            var network1 = CreateNetwork("output1.xml");
            var network2 = CreateNetwork("output2.xml");

            Console.WriteLine("Starting Network 1");
            Task t1 = network1.ExecuteAsync();
            Console.WriteLine("Starting Network 2");
            Task t2 = network2.ExecuteAsync();

            Console.WriteLine("Awaiting both networks");
            await Task.WhenAll(t1, t2);
            Console.WriteLine("Network 1 & 2 complete");
        }


        public static void WaitAllForTwoNetworks() {
            Console.WriteLine("Creating Network 1 & 2");
            var network1 = CreateNetwork("output1.xml");
            var network2 = CreateNetwork("output2.xml");

            //Act
            Task t1 = network1.ExecuteAsync();
            Task t2 = network2.ExecuteAsync();

            Console.WriteLine("Waiting for both networks");
            Task.WaitAll(t1, t2);
            Console.WriteLine("Network 1 & 2 complete");
        }
    }
}
