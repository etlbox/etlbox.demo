using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ComplexFlow
{
    public class Order
    {
        public string Number { get; set; }

        public string Item { get; set; }

        [RetrieveColumn("Key")]
        [GroupColumn("CustomerKey")]
        public int CustomerKey { get; set; }

        [MatchColumn("Name")]
        public string CustomerName { get; set; }

        [AggregateColumn("TotalAmount", AggregationMethod.Sum)]
        public decimal Amount { get; set; }
    }

    public class Customer
    {
        [ColumnMap("CustomerKey")]
        public int Key { get; set; }

        public string Name { get; set; }
    }

    public class Rating
    {
        public int CustomerKey { get; set; }

        public decimal TotalAmount { get; set; }

        [ColumnMap("Rating")]
        public string RatingValue => TotalAmount > 50 ? "A" : "F";
    }
}
