namespace EF_BenchmarkExample;
public class BenchmarkDbContext : DbContext
{
    public DbSet<BenchmarkRow> BenchmarkRows { get; set; }

    public ConnectionType ConnectionType { get; set; } = ConnectionType.SqlServer;

    public BenchmarkDbContext() {
    }

    public BenchmarkDbContext(ConnectionType connectionType) {
        ConnectionType = connectionType;
    }

    public static string SqlConnectionString { get; set; } = $"Data Source=(LocalDB)\\MSSQLLocalDB2022;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=efbenchmark;TrustServerCertificate=true;Connection Timeout=5;Encrypt=false;";
    public static string PostgresConnectionString { get; set; } = $"Server=localhost;Database=efbenchmark;User Id=postgres;Password=etlboxpassword;";
    public static string MySqlConnectionString { get; set; } = $"Server=localhost;Port=3306;Database=efbenchmark;Uid=root;Pwd=etlboxpassword;";

    //public static SqlConnectionManager SqlConnectionManager => new SqlConnectionManager(SqlConnectionString);
    protected override void OnConfiguring(DbContextOptionsBuilder options) {
        if (ConnectionType == ConnectionType.SqlServer)
            options.UseSqlServer(SqlConnectionString);
        //else 
        //if (ConnectionType == ConnectionType.Postgres)
        //    options.UseNpgsql(PostgresConnectionString);
        //else 
        //if (ConnectionType == ConnectionType.MySql)
        //    options.UseMySQL(MySqlConnectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder) {

    }
}

