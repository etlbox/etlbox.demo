namespace ConfigurationDrivenETL;

public class AccountRecord {
    public string? AccountNumber { get; set; }
    public string? Name { get; set; }
    public decimal? Balance { get; set; }
    public string? Currency { get; set; } = "EUR";
}
