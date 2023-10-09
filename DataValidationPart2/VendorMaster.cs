using ETLBox;
using Microsoft.Extensions.Primitives;

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

        [DbColumnMap("Id")]
        [ValueGenerationColumn]
        public int? DbId { get; set; }
        public bool IsInDb => DbId.HasValue && DbId > 0;

        public VendorMaster Normalize() {
            this.Name = Name.Trim().ToUpper();
            this.Contact = Contact.Trim();
            return this;
        }

        public bool IsValid() {

            if (string.IsNullOrEmpty(Name) || string.IsNullOrEmpty(Code) || string.IsNullOrEmpty(Custom)) {
                ErrorMessage = "Empty required column detected!";
                return false;
            }
            if (Name.Length < 5 || Name.Length > 50) {
                ErrorMessage = "Name has unsupported length!";
                return false;
            }
            if (Code.Length != 5) {
                ErrorMessage = "Code has unsupported length!";
                return false;
            }
            return true;
        }

        public string ErrorMessage { get; set; }

        public DateTime ValidFrom { get; set; } = new DateTime(1900,1,1);
        public DateTime ValidTo { get; set; } = new DateTime(9999,12,31);

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
