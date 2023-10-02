
namespace GettingStarted;
public class Blog
{
    public int BlogId { get; set; }
    public string Url { get; set; }


    public int? Rating { get; set; }

    public string DisplayUrl { get; set; }
    [Column("Created")]
    public DateTime? CreationDate { get; set; }
    [Column("Updated")]
    public DateTime? UpdateDate { get; set; }
    
    [NotMapped]
    public DateTime ChangeDate { get; set; }
    [NotMapped]
    public ChangeAction? ChangeAction { get; set; }
}
