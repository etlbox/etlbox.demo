using ETLBox;

namespace DataValidationPart1
{
    public class VendorMaster
    {

        [DbColumnMap("VendorName")]
        public string Name { get; set; }

        [DistinctColumn]
        public string Code { get; set; }

        [DistinctColumn]
        public string Custom { get; set; }

        public string Country { get; set; }

        public string Contact { get; set; }

        [CsvHelper.Configuration.Attributes.Name("TraceInfo")]
        public string Info { get; set; }

        public int? DbId { get; set; }
        public bool IsInDb => DbId.HasValue && DbId > 0;

        public VendorMaster Normalize() {
            this.Name = Name.Trim().ToUpper();
            this.Contact = Contact.Trim();
            return this;
        }

        public bool IsValid() {

            if (string.IsNullOrEmpty(Name) || string.IsNullOrEmpty(Code) || string.IsNullOrEmpty(Custom))
                return false;
            if (Name.Length < 5 || Name.Length > 50)
                return false;
            if (Code.Length != 5)
                return false;
            return true;
        }

    }

    public class VendorMasterDbEntry
    {
        [RetrieveColumn(nameof(VendorMaster.DbId))]
        public int Id { get; set; }
        [MatchColumn(nameof(VendorMaster.Code))]
        public string Code { get; set; }
        [MatchColumn(nameof(VendorMaster.Custom))]
        public string Custom { get; set; }
    }
}
