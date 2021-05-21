using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBoxDemo.WebScraping
{
    public class Accident
    {
        public DateTime Date { get; set; }
        public int Year => Date.Year;
        public string Type { get; set; }
        public string Registration { get; set; }
        public string Operator { get; set; }
        public int Fatalities { get; set; } = 0;
        public string Location { get; set; }
        public string Country { get; set; }
        public string Category { get; set; }
    }

    public class AccidentsPerYear 
    {        
        [GroupColumn("Year")]
        public int Year { get; set; }
        [AggregateColumn("Fatalities", AggregationMethod.Sum)]
        public int Fatalities { get; set; }
    }

    public class Program
    {
        public static SQLiteConnectionManager SQLiteConnection { get; set; }
                
        static void PrepareSqlLiteDestination() {
            SQLiteConnection = new SQLiteConnectionManager("Data Source=.\\SQLite.db;Version=3;");
            CreateTableTask.Create(SQLiteConnection, new TableDefinition() {
                Name = "Accidents",
                Columns = new List<TableColumn>() {
                    new TableColumn() { Name = "Date", DataType = "INTEGER" },
                    new TableColumn() { Name = "Type", DataType = "TEXT" },
                    new TableColumn() { Name = "Registration", DataType = "TEXT" },
                    new TableColumn() { Name = "Operator", DataType = "TEXT" },
                    new TableColumn() { Name = "Fatalities", DataType = "TEXT" },
                    new TableColumn() { Name = "Location", DataType = "TEXT" },
                    new TableColumn() { Name = "Country", DataType = "TEXT" },
                    new TableColumn() { Name = "Category", DataType = "TEXT" }
                }
            });
        }

        public static int StartYear = 1920;
        public static int EndYear = 1940;

        public static void Main(string[] args) {

            PrepareSqlLiteDestination();

            var currentYear = StartYear;

            var source = new CustomBatchSource<Accident>();
            source.ReadBatchFunc = _ => {
                var accidents = ParseAccidentsFromUrl($"https://aviation-safety.net/database/dblist.php?Year={currentYear}");
                currentYear++;
                return accidents;
            };
            source.ReadingCompleted = _ => currentYear > EndYear;

            var filter = new FilterTransformation<Accident>();
            filter.FilterPredicate = accident => accident.Year <= 1;

            var multicast = new Multicast<Accident>();
            
            var memDest = new MemoryDestination<Accident>();
            
            var sqlLiteDest = new DbDestination<Accident>(SQLiteConnection,"Accidents");
                        
            var aggregation = new Aggregation<Accident, AccidentsPerYear>();            
            var csvDest = new CsvDestination<AccidentsPerYear>("aggregated.csv");

            source.LinkTo(filter);
            filter.LinkTo(multicast);
            multicast.LinkTo(memDest);            
            multicast.LinkTo(sqlLiteDest);

            multicast.LinkTo(aggregation, row => row.Year > 1);
            aggregation.LinkTo(csvDest);

            Network.Execute(source);

            Console.WriteLine($"Imported {memDest.Data.Count} rows from aviation-safety.net");
            for (int year = StartYear;year<=EndYear;year++)
                Console.WriteLine($"There were {memDest.Data.Where(a => a.Year == year).Count()} accidents in {year}");
        }



        static List<Accident> ParseAccidentsFromUrl(string url) {
            var web = new HtmlWeb();
            var doc = web.Load(url);
            var rows = doc.DocumentNode.SelectNodes("//tr").Skip(1);

            var result = new List<Accident>();
            foreach (var r in rows) {
                var data = r.ChildNodes.Where(cn => cn.Name == "td");
                var accident = new Accident();
                accident.Date = ConvertToDate(data.ElementAt(0).InnerText);
                accident.Type = data.ElementAt(1).InnerText;
                accident.Registration = data.ElementAt(2).InnerText;
                accident.Operator = data.ElementAt(3).InnerText;
                accident.Fatalities = ConvertToNumber(data.ElementAt(4).InnerText);
                accident.Location = data.ElementAt(5).InnerText;
                accident.Country = ParseCountryFromImgTag(data.ElementAt(6).InnerHtml);
                accident.Category = data.ElementAt(8).InnerText;
                result.Add(accident);
            }

            return result;
        }

        static string ParseCountryFromImgTag(string imgHtml) {
            //imgHtml: <img src="//cdn.aviation-safety.net/database/country/flags_15/I.gif" title="Italy">
            if (imgHtml.Length > 0)
                return imgHtml.Substring(imgHtml.LastIndexOf("title=\"") + 7).Replace("\">","");
            else
                return string.Empty;
        }

        static int ConvertToNumber(string number) {            
            //number: 5+1
            if (number.Contains("+")) {
                var numbers = number.Split("+");
                return Convert.ToInt32(numbers[0]) + Convert.ToInt32(numbers[1]);
            }            
            //number: 3
            else if (!string.IsNullOrWhiteSpace(number))
                return Convert.ToInt32(number);
            //number: ""
            else
                return 0;
        }

        static DateTime ConvertToDate(string dateString) {
            //dateString: ??-???-1921
            //dateString: ??-DEC-1925
            if (dateString.StartsWith("??"))
                return new DateTime(Convert.ToInt32(dateString.Substring(7)));
            //dateString: 29-FEB-1950, did not exist
            else if (dateString == "29-FEB-1950")
                return new DateTime(1950, 2, 28);
            //dateString: 18-JAN-1942
            else
                return DateTime.Parse(dateString);
        }
    }
}
