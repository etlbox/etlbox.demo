using ETLBox;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataValidationExample
{
    public class CustomerRow
    {
        public string CustomerName { get; set; }

        [DistinctColumn]
        public string Code1 { get; set; }

        [DistinctColumn]
        public string Code2 { get; set; }

        public string Country { get; set; }


        public int? DbId { get; set; }
        public bool IsInDb => DbId.HasValue && DbId > 0;

        public string ErrorMessage { get; set; }    
        public bool IsValid() {
            if (string.IsNullOrEmpty(CustomerName) ||
                string.IsNullOrEmpty(Code1) ||
                string.IsNullOrEmpty(Code2))
                return false;
            if (CustomerName.Length < 5 || CustomerName.Length > 50)
                return false;
            if (Code1.Length != 5)
                return false;
            return true;
        }

    }

    public class CustomerDbEntry
    {
        [RetrieveColumn(nameof(CustomerRow.DbId))]
        public int Id { get; set; }
        [MatchColumn(nameof(CustomerRow.Code1))]
        public string Code1 { get; set; }
        [MatchColumn(nameof(CustomerRow.Code2))]
        public string Code2 { get; set; }
    }
}
