using ETLBox;

namespace SsisToCodeFirst
{
    public class OrderRow
    {
        public int OrderId { get; set; }
        public DateTime OrderDate { get; set; }

        [DbColumnMap(IgnoreColumn = true)]
        public string CustomerCode { get; set; }

        public int? CustomerId { get; set; }
        public string CustomerName { get; set; }
        public string ProductSku { get; set; }
        public int Quantity { get; set; }
        public decimal? UnitPrice { get; set; }

        public bool IsComplete => CustomerId != null && UnitPrice != null;
    }

    public class Customer
    {
        [MatchColumn(nameof(OrderRow.CustomerCode))]
        public string Code { get; set; }

        [RetrieveColumn(nameof(OrderRow.CustomerId))]
        public int Id { get; set; }

        [RetrieveColumn(nameof(OrderRow.CustomerName))]
        public string Name { get; set; }
    }

    public class ProductPrice
    {
        public string Sku { get; set; }
        public decimal Price { get; set; }
        public DateTime ValidFrom { get; set; }
        public DateTime ValidTo { get; set; }
    }
}
