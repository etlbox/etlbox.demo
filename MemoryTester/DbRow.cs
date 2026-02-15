using ETLBox;
using ETLBox.DataFlow;

namespace MemoryTester
{
    public class DbRow : MergeableRow
    {
        [IdColumn]
        public long Id { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue1 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue2 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue3 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue4 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue5 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue6 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue7 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue8 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue9 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public long LongValue10 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public string StringValue1 { get; set; }
        
        [CompareColumn]
        [UpdateColumn]
        public string StringValue2 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public string StringValue3 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public string StringValue4 { get; set; }

        [CompareColumn]
        [UpdateColumn]
        public string StringValue5 { get; set; }

        [DeleteColumn(DeleteOnMatchValue =true)]
        public bool DeleteFlag { get; set; }
    }
}
