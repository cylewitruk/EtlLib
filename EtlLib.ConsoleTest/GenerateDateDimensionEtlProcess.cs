using System;
using System.Globalization;
using System.Text;
using Amazon;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes.AmazonS3;
using EtlLib.Nodes.CsvFiles;
using EtlLib.Pipeline.Builders;

namespace EtlLib.ConsoleTest
{
    public class GenerateDateDimensionEtlProcess
    {
        private static readonly Calendar Calendar;

        static GenerateDateDimensionEtlProcess()
        {
            Calendar = new GregorianCalendar();
        }

        public static void Create(ILoggingAdapter loggingAdapter)
        {
            var startDate = DateTime.ParseExact("2000-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                CultureInfo.InvariantCulture);
            var endDate = DateTime.ParseExact("2025-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                CultureInfo.InvariantCulture);

            var process = EtlProcessBuilder.Create(cfg =>
                {
                    cfg
                        .WithLoggingAdapter(loggingAdapter)
                        .Named("Generate Date Dimension");
                })
                .GenerateInput<Row, DateTime>(
                    gen => gen.State <= endDate,
                    (i, gen) =>
                    {
                        if (i == 1)
                            gen.SetState(startDate);

                        var row = CreateRowFromDateTime(gen.State);
                        gen.SetState(gen.State.AddDays(1));

                        return row;
                    }
                )
                .Continue(ctx => new CsvWriterNode((string) ctx.StateDict["d_date_csv.csv"])
                    .IncludeHeader()
                    .WithEncoding(Encoding.UTF8))
                .Continue(ctx => new AmazonS3WriterNode(***REMOVED***, (string) ctx.StateDict["s3_bucket_name"])
                    .WithBasicCredentials((string) ctx.StateDict["s3_access_key_id"],
                        (string) ctx.StateDict["s3_access_key_secret"])
                );
        }

        public static Row CreateRowFromDateTime(DateTime date)
        {
            var isoWeekOfYear = CalculateIso8601WeekOfYear(date);
            var weekOfYear = CalculateWeekOfYear(isoWeekOfYear);
            var quarter = new Quarter(date);

            var row = new Row
            {
                ["date"] = date,
                ["epoch"] = (long) date.Subtract(new DateTime(1970, 1, 1)).TotalSeconds,
                ["d_date_key"] = int.Parse(date.ToString("yyyyMMdd", CultureInfo.InvariantCulture)),
                ["day_suffix"] = date.ToString("ddd", CultureInfo.InvariantCulture),
                ["day_name"] = date.ToString("dddd", CultureInfo.InvariantCulture),
                ["day_of_week"] = CalculateDayOfWeek(),
                ["day_of_month"] = int.Parse(date.ToString("dd", CultureInfo.InvariantCulture)),
                ["week_of_year"] = isoWeekOfYear,
                ["week_of_year_iso"] = $"{weekOfYear}-W{isoWeekOfYear}",
                ["week_year"] = weekOfYear,
                ["month"] = date.Month,
                ["year"] = date.Year,
                ["month_name"] = date.ToString("MMMM", CultureInfo.InvariantCulture),
                ["month_name_short"] = date.ToString("MMM", CultureInfo.InvariantCulture),
                ["week_of_month"] = CalculateActualWeekOfYear(date) - CalculateActualWeekOfYear(date.AddDays(1 - date.Day)) + 1,
                ["is_weekend"] = IsWeekend() ? 1 : 0,
                ["day_of_year"] = date.DayOfYear,
                ["quarter"] = quarter.QuarterNumber,
                ["quarter_name"] = quarter.QuarterName,
                ["quarter_name_short"] = quarter.QuarterNameShort,
                ["day_of_quarter"] = quarter.DayOfQuarter,
                ["fist_day_of_quarter"] = quarter.StartDate.ToString("yyyy-MM-dd"),
                ["last_day_of_quarter"] = quarter.EndDate.ToString("yyyy-MM-dd"),
                ["first_day_of_week"] = date.AddDays(CalculateDayOfWeek() - 1).ToString("yyyy-MM-dd"),
                ["last_day_of_week"] = date.AddDays(7 - CalculateDayOfWeek()).ToString("yyyy-MM-dd"),
                ["first_day_of_month"] = new DateTime(date.Year, date.Month, 1).ToString("yyyy-MM-dd"),
                ["last_day_of_month"] = new DateTime(date.Year, date.Month, Calendar.GetDaysInMonth(date.Year, date.Month)).ToString("yyyy-MM-dd"),
                ["first_day_of_year"] = new DateTime(date.Year, 1, 1).ToString("yyyy-MM-dd"),
                ["last_day_of_year"] = new DateTime(date.Year, 12, Calendar.GetDaysInMonth(date.Year, 12)).ToString("yyyy-MM-dd"),
                ["yyyymm"] = date.ToString("yyyyMM", CultureInfo.InvariantCulture),
                ["yyyymmdd"] = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture)
            };

            return row;

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