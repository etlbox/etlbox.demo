namespace ETLBoxDemo.SimpeFlow {
    public class MyRow {
        [CsvHelper.Configuration.Attributes.Name("rownr")]
        public string Id { get; set; }
        public string name { get; set; }
        public string quantity_m { get; set; }
        public string quantity_l { get; set; }
        public string price_in_cents { get; set; }
    }
}
