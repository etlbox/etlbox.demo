using CsvHelper.Configuration.Attributes;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LookupExamples
{
    internal class AdditionalExamples
    {
        public class InputRow
        {
            public string Id { get; set; }
            public string SomeValue { get; set; }
            public string ValueFromLookup { get; set; }
        }

        public class LookupRow
        {
            [MatchColumn("Id")]
            public string LookupId { get; set; }
            [RetrieveColumn("ValueFromLookup")]
            public string LookupValue { get; set; }

        }

        public void UsingGetInputRecordKeyFunc()
        {
            var source = new CsvSource<InputRow>("InputData.csv");
            source.Configuration.MissingFieldFound = null;

            var lookupSource = new MemorySource<LookupRow>();
            lookupSource.DataAsList = new List<LookupRow>() {
            new LookupRow() { LookupId = "idstringa", LookupValue = "A" },
            new LookupRow() { LookupId = "idstringb", LookupValue = "B" },
            new LookupRow() { LookupId = "idstringc", LookupValue = "C" }
        };

            var lookup = new LookupTransformation<InputRow, LookupRow>();
            lookup.Source = lookupSource;
            lookup.GetInputRecordKeyFunc = row => row.Id.ToLower();
            lookup.GetSourceRecordKeyFunc = row => row.LookupId;
            var dest = new CsvDestination<InputRow>("output1.csv");

            source.LinkTo(lookup).LinkTo(dest);

            Network.Execute(source);
                       
            PrintFile("InputData.csv");            
            PrintFile("output1.csv");
        }

        private static void PrintFile(string filename)
        {
            Console.WriteLine(Environment.NewLine + $"{filename} content:");
            foreach (var line in File.ReadLines(filename))
                Console.WriteLine(line);
        }


        /*
         * Internally, the lookup stores an object as dictionary key. If the used object for the key overrides
         * GetHashCode & Equals, this can be overwritten to define own matching logic.          
         */
        public class MyInputRow
        {
            public string Id { get; set; }
            [Ignore]
            public ComparableObject ConvertedId => new ComparableObject(Id);
            public string SomeValue { get; set; }
            public string ValueFromLookup { get; set; }
        }

        public class MyLookupRow
        {
            [MatchColumn("ConvertedId")]
            public ComparableObject LookupId { get; set; }
            [RetrieveColumn("ValueFromLookup")]
            public string LookupValue { get; set; }

        }

        public class ComparableObject
        {
            public string Id { get; set; }
            public ComparableObject(string id)
            {
                Id = id;
            }
            public override int GetHashCode()
            {
                return Id.ToLower().GetHashCode();
            }

            public override bool Equals(object obj)
            {
                var comp = obj as ComparableObject;
                if (comp == null) return false;
                return comp.Id.ToLower() == this.Id.ToLower();
            }
        }

        public void OverwritingComparisonInObject()
        {
            var source = new CsvSource<MyInputRow>("InputData.csv");
            source.Configuration.MissingFieldFound = null;

            var lookupSource = new MemorySource<MyLookupRow>();
            lookupSource.DataAsList = new List<MyLookupRow>() {
                new MyLookupRow() { LookupId = new ComparableObject("idstringa"), LookupValue = "A" },
                new MyLookupRow() { LookupId = new ComparableObject("idstringb"), LookupValue = "B" },
                new MyLookupRow() { LookupId = new ComparableObject("idstringc"), LookupValue = "C" }
            };

            var lookup = new LookupTransformation<MyInputRow, MyLookupRow>();
            lookup.Source = lookupSource;

            var dest = new CsvDestination<MyInputRow>("output2.csv");

            source.LinkTo(lookup).LinkTo(dest);

            Network.Execute(source);

            PrintFile("InputData.csv");
            PrintFile("output1.csv");
        }

    }
}
