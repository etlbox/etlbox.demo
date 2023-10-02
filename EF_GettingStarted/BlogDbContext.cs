
namespace GettingStarted;
public class BloggingContext : DbContext
{
    public DbSet<Blog> Blogs { get; set; }

    public string DbPath { get; }

    public BloggingContext() {
    }

    protected override void OnConfiguring(DbContextOptionsBuilder options) {
        options.UseSqlServer($"Data Source=localhost;User Id=sa;Password=YourStrong@Passw0rd;Initial Catalog=efdemo;TrustServerCertificate=true");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder) {
        modelBuilder.Entity<Blog>()
            .Property(b => b.Rating)
            .HasDefaultValue(3);
        modelBuilder.Entity<Blog>()
            .Property(p => p.DisplayUrl)
             .HasComputedColumnSql("'https:'+[Url]");
        modelBuilder.Entity<Blog>()
        .Property(b => b.CreationDate)
        .HasDefaultValueSql("GETDATE()");

    }
}
