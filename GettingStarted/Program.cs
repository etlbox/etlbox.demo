/* Create an initial migration first & update database
 * 
Add-Migration InitialCreate
Update-Database

Guide: https://learn.microsoft.com/en-us/ef/core/managing-schemas/migrations/?tabs=dotnet-core-cli

*/

using ETLBox.EntityFramework.SqlServer;

namespace GettingStarted;

public class Program
{
    public static void Main(string[] args) {
        using (var db = new BloggingContext()) {
            db.Database.ExecuteSqlRaw("TRUNCATE TABLE Blogs");
            // Note: This sample requires the database to be created before running.

            BulkInsertUpdateDelete(db, rowsToInsert: 5000, rowsToUpdate: 4000, rowsToDelete: 1000);

            BulkMerge(db, rowsToInsert: 1000, rowsToUpdate: 1000, rowsToDelete: 1000);
        }
    }

    static void BulkInsertUpdateDelete(BloggingContext db, int rowsToInsert, int rowsToUpdate, int rowsToDelete) {
        //Creating demo data
        var data = new List<Blog>();
        for (int i = 1; i <= rowsToInsert; i++) {
            var newBlog = new Blog {
                Url = $"blogs.msdn.com/{i}",
                Rating = i % 10
            };
            data.Add(newBlog);
        }

        //Bulk insert
        Console.WriteLine($"Bulk inserting {rowsToInsert} rows.");
        db.BulkInsert(data);
        Console.WriteLine($"Bulk insert successful.");
        Console.WriteLine($"Highest BlogId:" + data.Max(b => b.BlogId));
        Console.WriteLine($"Highest CreationDate:" + data.Max(b => b.CreationDate));

        //Updating demo data
        for (int i=0;i< rowsToUpdate;i++) {
            data[i].Rating = data[i].Rating * 10;
            data[i].Url = "https://" + data[i].Url;
            data[i].UpdateDate = DateTime.Now.ToUniversalTime();
        }

        //Bulk update
        
        Console.WriteLine($"Bulk updating {rowsToUpdate} rows .");
        db.BulkUpdate(data.Take(rowsToUpdate));
        Console.WriteLine($"Bulk update successful.");
        Console.WriteLine($"Highest UpdateDate:" + data.Max(b => b.UpdateDate));

        //Bulk delete
        Console.WriteLine($"Bulk deleting {rowsToDelete} rows.");
        db.BulkDelete(data.Take(rowsToDelete));
        Console.WriteLine($"Bulk delete successful.");
    }

    static void BulkMerge(BloggingContext db, int rowsToInsert, int rowsToUpdate, int rowsToDelete) {
        //Load existing data 
        var data = db.Blogs.ToList();

        //Delete first X records from start
        data.RemoveRange(0, rowsToDelete);

        //Update first X records
        for (int i = 0; i < rowsToUpdate; i++) {
            data[i].Url = null;
            data[i].Rating = -1;
            data[i].UpdateDate = DateTime.Now.ToUniversalTime();
        }

        //Append new records at end
        for (int i = 0; i <= rowsToInsert; i++) {
            var newBlog = new Blog {
                Url = $"blogs.msdn.com/{i}",                
                CreationDate = DateTime.Now.ToUniversalTime(),                
            };
            data.Add(newBlog);
        }

        //Sync with table
        Console.WriteLine($"Bulk merging {data.Count()} rows with destination.");
        db.BulkMerge(data, options => options.MergeMode = MergeMode.Full);
        Console.WriteLine($"Bulk merge successful.");

    }
}
