namespace EF_BenchmarkExample;
public class BenchmarkDbContext : DbContext
{
    public DbSet<BenchmarkRow> BenchmarkRows { get; set; }

    public BenchmarkDbContext() {
    }

    public static string ConnectionString = $"Data Source=(LocalDB)\\MSSQLLocalDB2022;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=efbenchmark;TrustServerCertificate=true;Connection Timeout=5;Encrypt=false;";
    public static SqlConnectionManager SqlConnectionManager => new SqlConnectionManager(ConnectionString);
    protected override void OnConfiguring(DbContextOptionsBuilder options) {
        options.UseSqlServer(ConnectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder) {

    }
}
