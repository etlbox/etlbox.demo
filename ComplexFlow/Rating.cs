using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ComplexFlow
{
    public class Rating
    {
        public int CustomerKey { get; set; }
        public decimal TotalAmount { get; set; }
        [ColumnMap("Rating")]
        public string RatingValue { get; set; }
    }
}
