namespace DataValidationPart3
{
    public class Meta
    {
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public bool IsMandatory { get; set; }
        public bool IsBusinessKey { get; set; }
        public string FileColumnName { get; set; }
        public int MinFieldLength { get; set; }
        public int MaxFieldLength { get; set; }
        public bool Trim { get; set; }
        public bool Uppercase { get; set; }
    }
}
