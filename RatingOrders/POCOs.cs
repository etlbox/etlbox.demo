﻿using ETLBox;

namespace ETLBoxDemo.RatingOrdersExample
{
    public class Order
    {
        public string Number { get; set; }
        public string Item { get; set; }
        public int CustomerKey { get; set; }
        public string CustomerName { get; set; }
        public decimal Amount { get; set; }
    }

    public class Customer
    {
        [RetrieveColumn(nameof(Order.CustomerKey))]
        public int Key { get; set; }
        [MatchColumn(nameof(Order.CustomerName))]
        public string Name { get; set; }
    }

    public class Rating
    {
        [GroupColumn(nameof(Order.CustomerKey))]
        public int CustomerKey { get; set; }
        [AggregateColumn(nameof(Order.Amount), AggregationMethod.Sum)]
        public decimal TotalAmount { get; set; }

        [DbColumnMap("Rating")]
        public string RatingValue => TotalAmount > 50 ? "A" : "F";
    }
}
