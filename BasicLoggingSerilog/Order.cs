using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BasicLoggingSerilog {
    public class Order {
        public int OrderId { get; set; }
        public string Item { get; set; }
        public int Quantity { get; set; }
        public string CustomerName { get; set; }
        public int? CustomerId { get; set; }
    }
}
