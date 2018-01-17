using System;
using System.Globalization;
using System.Text;
using Amazon;
using EtlLib.Data;
using EtlLib.Nodes.AmazonS3;
using EtlLib.Nodes.CsvFiles;
using EtlLib.Nodes.FileCompression;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.ConsoleTest
{
    public class GenerateDateDimensionEtlProcess : AbstractEtlProcess
    {
        private static readonly Calendar Calendar;

        static GenerateDateDimensionEtlProcess()
        {
            Calendar = new GregorianCalendar();
        }

        public GenerateDateDimensionEtlProcess(string s3BucketName, string s3AccessKeyId, string s3AccessKeySecret, string outputFilePath)
        {
            var startDate = DateTime.ParseExact("2000-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                CultureInfo.InvariantCulture);
            var endDate = DateTime.ParseExact("2025-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                CultureInfo.InvariantCulture);

            Build(builder =>
            {
                builder
                    .Named("Generate Date Dimension")
                    .GenerateInput<Row, DateTime>(
                        gen => gen.State <= endDate,
                        (ctx, i, gen) =>
                        {
                            if (i == 1)
                                gen.SetState(startDate);

                            var row = ctx.ObjectPool.Borrow<Row>();
                            CreateRowFromDateTime(row, gen.State);
                            gen.SetState(gen.State.AddDays(1));

                            return row;
                        }
                    )
                    .Continue(ctx => new CsvWriterNode(outputFilePath)
                        .IncludeHeader()
                        .WithEncoding(Encoding.UTF8))
                    .BZip2Files(cfg => cfg
                        .CompressionLevel(9)
                        .Parallelize(2)
                        .FileSuffix(".bzip2"));
                //.Continue(ctx => new AmazonS3WriterNode(***REMOVED***, _s3BucketName)
                //    .WithBasicCredentials(_s3AccessKeyId, _s3AccessKeySecret)
                //);
            });
        }

        public static void CreateRowFromDateTime(Row row, DateTime date)
        {
            var isoWeekOfYear = CalculateIso8601WeekOfYear(date);
            var weekOfYear = CalculateWeekOfYear(isoWeekOfYear);
            var quarter = new Quarter(date);
            
            row["date"] = date;
            row["epoch"] = (long) date.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;
            row["d_date_key"] = int.Parse(date.ToString("yyyyMMdd"));
            row["day_suffix"] = date.ToString("ddd", CultureInfo.InvariantCulture);
            row["day_name"] = date.ToString("dddd", CultureInfo.InvariantCulture);
            row["day_of_week"] = CalculateDayOfWeek();
            row["day_of_month"] = int.Parse(date.ToString("dd", CultureInfo.InvariantCulture));
            row["week_of_year"] = isoWeekOfYear;
            row["week_of_year_iso"] = $"{weekOfYear}-W{isoWeekOfYear}";
            row["week_year"] = weekOfYear;
            row["month"] = date.Month;
            row["year"] = date.Year;
            row["month_name"] = date.ToString("MMMM", CultureInfo.InvariantCulture);
            row["month_name_short"] = date.ToString("MMM", CultureInfo.InvariantCulture);
            row["week_of_month"] = CalculateActualWeekOfYear(date) -
                                   CalculateActualWeekOfYear(date.AddDays(1 - date.Day)) + 1;
            row["is_weekend"] = IsWeekend() ? 1 : 0;
            row["day_of_year"] = date.DayOfYear;
            row["quarter"] = quarter.QuarterNumber;
            row["quarter_name"] = quarter.QuarterName;
            row["quarter_name_short"] = quarter.QuarterNameShort;
            row["day_of_quarter"] = quarter.DayOfQuarter;
            row["fist_day_of_quarter"] = quarter.StartDate.ToString("yyyy-MM-dd");
            row["last_day_of_quarter"] = quarter.EndDate.ToString("yyyy-MM-dd");
            row["first_day_of_week"] = date.AddDays(-CalculateDayOfWeek() - 1).ToString("yyyy-MM-dd");
            row["last_day_of_week"] = date.AddDays(7 - CalculateDayOfWeek()).ToString("yyyy-MM-dd");
            row["first_day_of_month"] = new DateTime(date.Year, date.Month, 1).ToString("yyyy-MM-dd");
            row["last_day_of_month"] =
                new DateTime(date.Year, date.Month, Calendar.GetDaysInMonth(date.Year, date.Month)).ToString(
                    "yyyy-MM-dd");
            row["first_day_of_year"] = new DateTime(date.Year, 1, 1).ToString("yyyy-MM-dd");
            row["last_day_of_year"] =
                new DateTime(date.Year, 12, Calendar.GetDaysInMonth(date.Year, 12)).ToString("yyyy-MM-dd");
            row["yyyymm"] = date.ToString("yyyyMM");
            row["yyyymmdd"] = date.ToString("yyyyMMdd");

            int CalculateDayOfWeek()
            {
                var dayOfWeek = (int)date.DayOfWeek;
                return (dayOfWeek == 0) ? 7 : dayOfWeek;
            }

            int CalculateWeekOfYear(int isoWeek)
            {
                if (isoWeek >= 52 && date.Month == 1)
                    return date.Year - 1;
                if (isoWeek == 1 && date.Month == 12)
                    return date.Year + 1;

                return date.Year;
            }

            bool IsWeekend()
            {
                var dayOfWeek = (int)date.DayOfWeek;
                return (dayOfWeek == 0 || dayOfWeek == 6);
            }

            int CalculateIso8601WeekOfYear(DateTime d)
            {
                // Copied from
                // https://blogs.msdn.microsoft.com/shawnste/2006/01/24/iso-8601-week-of-year-format-in-microsoft-net/

                // Seriously cheat.  If its Monday, Tuesday or Wednesday, then it'll 
                // be the same week# as whatever Thursday, Friday or Saturday are,
                // and we always get those right
                var day = Calendar.GetDayOfWeek(d);
                if (day >= DayOfWeek.Monday && day <= DayOfWeek.Wednesday)
                {
                    d = d.AddDays(3);
                }

                // Return the week of our adjusted day
                return Calendar.GetWeekOfYear(d, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
            }

            int CalculateActualWeekOfYear(DateTime d)
            {
                return Calendar.GetWeekOfYear(date, CalendarWeekRule.FirstDay, DayOfWeek.Monday);
            }
        }
    }

    public class Quarter
    {
        public string QuarterName { get; }
        public string QuarterNameShort { get; }
        public int QuarterNumber { get; }
        public DateTime StartDate { get; }
        public DateTime EndDate { get; }
        public int DayOfQuarter { get; }

        public Quarter(DateTime date)
        {
            var month = date.Month;

            if (month >= 1 && month <= 3)
            {
                QuarterNumber = 1;
                QuarterName = "First";
                QuarterNameShort = "Q1";
                StartDate = CalculateStartDate(date);
                EndDate = CalculateEndDate(date, 3);

            }
            else if (month >= 4 && month <= 6)
            {
                QuarterNumber = 2;
                QuarterName = "Second";
                QuarterNameShort = "Q2";
                StartDate = CalculateStartDate(date);
                EndDate = CalculateEndDate(date, 6);
            }
            else if (month >= 7 && month <= 9)
            {
                QuarterNumber = 3;
                QuarterName = "Third";
                QuarterNameShort = "Q3";
                StartDate = CalculateStartDate(date);
                EndDate = CalculateEndDate(date, 9);
            }
            else if (month >= 10 && month <= 12)
            {
                QuarterNumber = 4;
                QuarterName = "Fourth";
                QuarterNameShort = "Q4";
                StartDate = CalculateStartDate(date);
                EndDate = CalculateEndDate(date, 12);
            }
            DayOfQuarter = (int)(date - StartDate).TotalDays;
        }

        private static DateTime CalculateStartDate(DateTime date)
        {
            return new DateTime(date.Year, date.Month, 1);
        }

        private static DateTime CalculateEndDate(DateTime date, int month)
        {
            return new DateTime(date.Year, month, DateTime.DaysInMonth(date.Year, month));
        }
    }
}