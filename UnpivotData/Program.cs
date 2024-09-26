using CsvHelper.Configuration.Attributes;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;

namespace PivotTest
{
    class Program
    {
        public class InputData
        {
            public int Account { get; set; }
            [Name("JAN")]
            public int January { get; set; }
            [Name("FEB")]
            public int February { get; set; }
            [Name("MAR")]
            public int March { get; set; }
        }

        public class PivotedOutput
        {
            public int Account { get; set; }
            public string Month { get; set; }
            public int MonthlyValue { get; set; }
            
        }

        static void Main(string[] args)
        {
            var source = new CsvSource<InputData>("Accounts_Quartal1.csv");
            source.Configuration.Delimiter = ";";

            var trans = new RowMultiplication<InputData, PivotedOutput>();
            trans.MultiplicationFunc = row =>
            {
                List<PivotedOutput> result = new List<PivotedOutput>();
                result.Add(new PivotedOutput()
                {
                    Account = row.Account,
                    Month = nameof(InputData.January),
                    MonthlyValue = row.January
                });
                result.Add(new PivotedOutput()
                {
                    Account = row.Account,
                    Month = nameof(InputData.February),
                    MonthlyValue = row.February
                });
                result.Add(new PivotedOutput()
                {
                    Account = row.Account,
                    Month = nameof(InputData.March),
                    MonthlyValue = row.March
                });
                return result;
            };
            var dest = new CsvDestination<PivotedOutput>("AccountNumbers_Pivoted.csv");
            dest.Configuration.HasHeaderRecord = false;

            source.LinkTo(trans);
            trans.LinkTo(dest);

            Network.Execute(source);

            /* AccountNumbers_Pivoted.csv output:             
                4711,January,10
                4711,February,11
                4711,March,12
                4712,January,20
                4712,February,21
                4712,March,22
                4713,January,30
                4713,February,31
                4713,March,32
            */
        }
    }
}
