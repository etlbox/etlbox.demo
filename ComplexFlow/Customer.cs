using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ComplexFlow
{
    public class Customer
    {
        public int CustomerKey { get; set; }
        [ColumnMap("Name")]
        public string CustomerName { get; set; }
    }
}
