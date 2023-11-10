using ETLBox;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBoxDemo.StarSchema;


public class DateDimension
{
    [DbColumnMap(IgnoreColumn =true)]
    public DateTime Date { get; set; }
    public int DateID => int.Parse(this.Date.ToString("yyyyMMdd"));
    public string DateString => this.Date.ToString("MM/dd/yyyy"); 
    public int Month => this.Date.Month; 
    public int Day => this.Date.Day; 
    public int Year => this.Date.Year; 
    public int DayofWeek => (int)this.Date.DayOfWeek; 
    public string DayofWeekName => this.Date.DayOfWeek.ToString(); 
    public int DayofYear => this.Date.DayOfYear; 
    public int WeekOfYear => this.GetWeekOfYear();
    public string MonthName => this.Date.ToString("MMMM");
    public int QuarterOfYear => this.GetQuarterOfYear();
            
    private int GetQuarterOfYear() {
        return (int)Math.Floor(((decimal)this.Date.Month + 2) / 3);
    }

    private int GetWeekOfYear() {
        System.Globalization.DateTimeFormatInfo dfi = System.Globalization.DateTimeFormatInfo.CurrentInfo;
        System.Globalization.Calendar cal = dfi.Calendar;
        return cal.GetWeekOfYear(this.Date, dfi.CalendarWeekRule, dfi.FirstDayOfWeek);
    }

    public static IEnumerable<DateDimension> Generate(DateTime StartDate, DateTime EndDate) {

        int TotalDays = (int)(EndDate.AddDays(1) - StartDate).TotalDays;
        return Enumerable.Range(0, TotalDays).Select(e =>
            new DateDimension() { Date = StartDate.AddDays(e) }
            ).OrderBy(e => e.DateID).ToList();
    }
}
