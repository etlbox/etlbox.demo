using System;
using System.Collections.Generic;
using System.Text;

namespace SimpeFlow
{
    public class Order
    {
        public int Id { get; set; }
        public string Item { get; set; }
        public int Quantity { get; set; }
        public double Price { get; set; }
    }
}
