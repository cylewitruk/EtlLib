using System;
using System.ComponentModel;
using System.Globalization;
using EtlLib.Data;

namespace EtlLib
{
    public static class RowExtensions
    {
        public static T GetAs<T>(this Row row, string key)
        {
            if (row[key] is T variable)
                return variable;

            var converter = TypeDescriptor.GetConverter(typeof(T));
            return (T)converter.ConvertFrom(row[key]);
        }

        public static T GetAs<T>(this Row row, string key, T defaultValue)
        {
            try
            {
                return (T) row[key];
            }
            catch (InvalidCastException)
            {
                return defaultValue;
            }
        }

        public static DateTime ParseDateTime(this Row row, string key, string format)
        {
            return DateTime.ParseExact(row.GetAs<string>(key), format, CultureInfo.InvariantCulture);
        }
    }
}