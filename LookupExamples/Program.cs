using System;

namespace LookupExamples
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Running examples from the docs!");
            DocsExamples de = new DocsExamples();
            de.UsingRowTransformation();
            de.UsingLookup();
            de.UsingLookupWithAttributes();
            de.AttributesWithDynamic();
            de.UsingLookupWithRetrievalByKeyFunc();
            de.PartialDbCacheWithAttributes();
            de.PartialDbCacheWithSql();
            Console.WriteLine("Done!");

            Console.WriteLine("Running additional examples!");
            AdditionalExamples ae = new AdditionalExamples();
            ae.UsingGetInputRecordKeyFunc();
            ae.OverwritingComparisonInObject();
            Console.WriteLine("Done!");

            Console.WriteLine("Running alternative examples!");
            AlternativeExamples alte = new AlternativeExamples();
            alte.UsingBatchTransformation();
            alte.UsingMergeJoin();
            Console.WriteLine("Done!");
        }
    }
}
