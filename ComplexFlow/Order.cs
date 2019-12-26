using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ComplexFlow
{
    public class Order
    {
        public string Number { get; set; }
        public string Item { get; set; }
        public decimal Amount { get; set; }
        public int CustomerKey { get; set; }
        public string CustomerName { get; set; }
        public Rating Rating { get; set; }
    }
}
