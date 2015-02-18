using System;
using System.Collections.Generic;
using System.Text;
using log4net;

using System.Text.RegularExpressions; // Regex
using System.Configuration; // ConfigurationManager
using System.Data; // DataTable
using System.IO; // Path

namespace NZ01
{

    /// <summary>
    /// AppUtility: Utility functions specific to this app
    /// </summary>
    public class AppUtility
    {
        private static log4net.ILog logger = log4net.LogManager.GetLogger("AppUtility");

        public static string formatError(Exception ex)
        {
            StringBuilder sbErrorHtml = new StringBuilder();

            sbErrorHtml.Append("<h2>Well, this is embarrassing...</h2><hr>");
            sbErrorHtml.Append("<p>Something has happened that the web-page code has not been written to deal with (called an Unhandled Exception). The diagnostic information below should help the developers figure out what went wrong.");
            sbErrorHtml.Append("<br /><br />");
            sbErrorHtml.Append("The error should have been logged automatically by the system, and emailed to the developers. However, this being an error and all that, it might not have worked, so if it persists it might help to copy and paste this info into an email and send it to...");
            sbErrorHtml.Append("<br /><br />");
            sbErrorHtml.Append("<a href=\"mailto:facelessdevteam@gmail.com\">FacelessDevTeam@gmail.com</a>");
            sbErrorHtml.Append("<br />");
            sbErrorHtml.Append("</p><hr><br />");

            formatMessage(sbErrorHtml, ex, 0);

            return sbErrorHtml.ToString();
        }

        private static void formatMessage(StringBuilder sbMessage, Exception ex, int iIndentSpaceCount)
        {
            String sIndentSpaces = new String('.', iIndentSpaceCount);

            // Exception.Message
            sbMessage.Append(sIndentSpaces);
            sbMessage.Append("Message: ");
            sbMessage.Append(ex.Message);
            sbMessage.Append("<br />");

            // Exception.Source
            sbMessage.Append(sIndentSpaces);
            sbMessage.Append("Source: ");
            sbMessage.Append(ex.Source);
            sbMessage.Append("<br />");


            // Exception.StackTrace
            sbMessage.Append(sIndentSpaces);
            sbMessage.Append("StackTrace:<br />");
            sbMessage.Append(sIndentSpaces);
            String sStackTrace = ex.StackTrace;
            String sWebStackTrace = sStackTrace.Replace("\r\n", "<br />" + sIndentSpaces);
            sbMessage.Append(sWebStackTrace);
            sbMessage.Append("<br />");


            sbMessage.Append("<br />");


            if (ex.InnerException != null)
            {
                ex = ex.InnerException;
                iIndentSpaceCount += 4;
                formatMessage(sbMessage, ex, iIndentSpaceCount);
            }
        }

        static public string FormatDateTimeAsFixUtcString(DateTime dt)
        {
            // DateTime Should Be UTC for FIX applications
            // http://msdn.microsoft.com/en-us/library/8kb3ddd4.aspx

            string sFixFormat = "yyyyMMdd-HH:mm:ss.fff";

            return dt.ToUniversalTime().ToString(sFixFormat);
        }

        static public string FormatDateTimeAsContiguousString(DateTime dt)
        {
            string sContiguousFormat = "yyyyMMddHHmmssfff";

            return dt.ToString(sContiguousFormat);
        }

        static public string FormatDateTimeAsUniversalDatabaseString(DateTime dt)
        {
            string sDBFormat = "yyyy-MM-ddTHH:mm:ss.fff";

            return dt.ToString(sDBFormat);
        }

        /// <summary>
        /// Given an Enumerable Collection of type T, build a single string, by calling T.ToString()
        /// </summary>
        /// <typeparam name="T">Type T; Must implement ToString()</typeparam>
        /// <param name="collection">Collection of type T; Must implement IEnumerable</param>
        /// <param name="noWhiteSpace">bool; If true, remove white space matching Regex \s (spaces,tabs,newlines)</param>
        /// <param name="maxLength">Int; Maximum output length; Ignored if negative or zero</param>
        /// <param name="sDelim">string; Delimiter char(s); Optional; Defaults to single comma character</param>
        /// <returns>string;</returns>
        public static string ConvertEnumerableCollectionToString<T>(IEnumerable<T> collection, bool noWhiteSpace, int maxLength, string sDelim = ",")
        {
            StringBuilder sb = new StringBuilder();
            int count = 0;

            if (noWhiteSpace)
                Regex.Replace(sDelim, @"\s+", " ").Trim();

            foreach (T item in collection)
            {
                string sItem = item.ToString();

                if (noWhiteSpace)
                    Regex.Replace(sItem, @"\s+", " ").Trim();

                // Work out how long the string will be if we add a new item.
                // Note delimiter is added only if we have been round loop once.
                int lengthWithThisItem = sb.Length + (count > 0 ? sDelim.Length : 0) + sItem.Length;

                // If there is a limit, and the limit is broken, break out and don't add any more items
                if (maxLength > 0 && lengthWithThisItem > maxLength)
                    break;
                // implicit else, continue...

                if (count > 0)
                    sb.Append(sDelim);

                sb.Append(sItem);
                ++count;
            }

            return sb.ToString();
        }

        /// <summary>
        /// Load an int value from config
        /// </summary>
        /// <param name="iValue">Reference to int value to receive</param>
        /// <param name="sKey">string; Entry in the config</param>
        public static void LoadIntVariable(ref int iValue, string sKey)
        {
            string sCandidate = ConfigurationManager.AppSettings[sKey];
            int iCandidate;
            if (Int32.TryParse(sCandidate, out iCandidate))
            {
                iValue = iCandidate;
            }
        }

        public static void LoadDecimalVariable(ref decimal value, string sKey)
        {
            string sCandidate = ConfigurationManager.AppSettings[sKey];
            decimal candidate;
            if (Decimal.TryParse(sCandidate, out candidate))
            {
                value = candidate;
            }
        }

        /// <summary>
        /// Convert a subset of selected rows from a DataTable to an object 
        /// that can be passed to a JavaScriptSerializer.Serialize()
        /// </summary>
        /// <param name="table">DataTable;</param>
        /// <returns>List of (Dictionary of string-to-object)</returns>
        public static List<Dictionary<string, object>> DataTableToList(DataTable table, DataRow[] rows)
        {
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();

            foreach (DataRow row in rows)
            {
                Dictionary<string, object> dict = new Dictionary<string, object>();

                foreach (DataColumn col in table.Columns)
                    dict[col.ColumnName] = (row[col]).ToString();

                list.Add(dict);
            }

            return list;
        }


       
        /// <summary>
        /// Convert a DataTable to an object that can be passed to a JavaScriptSerializer.Serialize()
        /// </summary>
        /// <param name="table">DataTable;</param>
        /// <returns>List of (Dictionary of string-to-object)</returns>
        /// 
        public static List<Dictionary<string, object>> DataTableToList(DataTable table)
        {
            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();

            foreach (DataRow row in table.Rows)
            {
                Dictionary<string, object> dict = new Dictionary<string, object>();

                foreach (DataColumn col in table.Columns)
                    dict[col.ColumnName] = (row[col]).ToString();

                list.Add(dict);
            }

            return list;
        }
        

        /// <summary>
        /// Dump a List of Dictionary-of-String-to-Object to log file
        /// </summary>
        /// <param name="listDic">List of type (Dictionary of String to Object)</param>
        /// <param name="sOptionalText">string; Additional text to mark items in the log file to help locate them.</param>
        /// <remarks>
        /// Used in diagnostics to see what is returned as DataTable rows.
        /// </remarks>
        public static void DumpList(List<Dictionary<string, object>> listDic, string sOptionalText = "")
        {
            string prefix;
            if (string.IsNullOrWhiteSpace(sOptionalText))
                prefix = "dumpList() - ";
            else
                prefix = "dumpList() [" + sOptionalText + "] - ";


            logger.Debug(prefix + "***** START LIST *****");

            int countDic = 0;
            foreach (Dictionary<string, object> dic in listDic)
            {
                ++countDic;
                int iOnce = 0;
                string sDic = string.Format("Dic {0}: ", countDic.ToString("D4"));
                foreach (KeyValuePair<string, object> entry in dic)
                {
                    if (iOnce > 0)
                        sDic += ", ";

                    sDic += entry.Key + ":" + entry.Value.ToString();

                    iOnce = 1;
                }

                logger.Debug(prefix + sDic);
            }

            logger.Debug(prefix +  "***** FINISH LIST *****");
        }

        /// <summary>
        /// Dump a collection to a single string for quick diagnostics
        /// </summary>
        /// <param name="dic">Collection object that implements IDictionary</param>
        /// <remarks>
        /// Format is based on JSON
        /// </remarks>
        public static string DumpToString<T, U>(IDictionary<T, U> dic)
        {
            string sReturn = string.Empty;

            sReturn += "[";
            int iOnce = 0;
            foreach (KeyValuePair<T, U> entry in dic)
            {
                if (iOnce > 0)
                    sReturn += ",";

                sReturn += "{";

                sReturn += entry.Key.ToString();
                sReturn += ":";
                sReturn += entry.Value.ToString();

                sReturn += "}";

                iOnce = 1;
            }

            sReturn += "]";

            return sReturn;
        }

        /// <summary>
        /// Convert a collection to a single comma separated string.
        /// </summary>
        /// <typeparam name="T">type T</typeparam>
        /// <param name="collectionOfTs">Collection of type T</param>
        /// <returns>string; CSV list of ToString() representation of contents of collection</returns>
        public static string ToCSVString<T>(ICollection<T> collectionOfTs, string quote = "")
        {
            StringBuilder sb = new StringBuilder();

            int once = 0;
            foreach (T t in collectionOfTs)
            {
                if (once == 1)
                    sb.Append(",");

                sb.Append(quote);
                sb.Append(t.ToString());
                sb.Append(quote);

                once = 1;
            }

            return sb.ToString();
        }


        /// <summary>
        /// Round a decimal value up or down to a set number of places, without leaving decimal type.
        /// </summary>
        /// <param name="input">decimal; Input value to round up or down</param>
        /// <param name="places">int; Number of decimal places</param>
        /// <param name="up">bool; If true, requests a round up, else, round down</param>
        /// <returns>decimal; input value rounded up or down to the correct number of places</returns>
        public static decimal Round(decimal input, int places, bool up = true)
        {
            if (places < 0) return input; // Remain sane

            decimal multiplier = 1;
            for (int i = 0; i < places; ++i)
                multiplier *= 10;

            if (up)
                return (Math.Ceiling(input * multiplier) / multiplier);
            else
                return (Math.Floor(input * multiplier) / multiplier);
        }







        /// <summary>
        /// Generate a string to express the TimeSpan as a shift to UTC
        /// </summary>
        /// <param name="offset">TimeSpan</param>
        /// <returns>string; String representation of timezone relative to UTC</returns>
        /// <remarks>
        /// Examples:
        ///   Offset == 0 hours; ---> "UTC"
        ///   Offset == 3 hours;  ---> "UTC+3"
        ///   Offset == -3 hours; ---> "UTC-3"
        ///   Offset == 3 hours 30 mins; ---> "UTC+3:30"
        /// </remarks>
        public static string GetUTCOffset(TimeSpan offset)
        {
            string sReturn = string.Empty;

            sReturn += "UTC";

            if (offset == TimeSpan.Zero)
                return sReturn;

            sReturn += (offset > TimeSpan.Zero) ? "+" : "-";
            sReturn += offset.ToString("hh");

            // If there is a minute part to the offset, add that
            if (offset.Minutes > 0)
            {
                sReturn += ":";
                sReturn += offset.ToString("mm");
            }

            return sReturn;
        }

        public static string GetUTCTimeStringFromTimeSpan(DateTime dtLocal, TimeSpan ts, bool bIncludeDate = false)
        {
            DateTime dtUTC = dtLocal.Subtract(ts);
            return GetCustomDateTimeString(dtUTC, bIncludeDate);
        }

        public static string GetLocalTimeStringFromTimeSpan(DateTime dtLocal, TimeSpan ts, TimeSpan tsBrowser, bool bIncludeDate = false)
        {
            DateTime dtUTC = dtLocal.Subtract(ts);
            DateTime dtBrowser = dtUTC.Add(tsBrowser);
            return GetCustomDateTimeString(dtBrowser, bIncludeDate);
        }

        public static string GetCustomDateTimeString(DateTime dt, bool bIncludeDate = false)
        {
            if (bIncludeDate)
            {
                if (dt.Millisecond == 0 && dt.Second == 0)
                    return dt.ToString("yyyy/MM/dd HH:mm");
                else if (dt.Millisecond == 0)
                    return dt.ToString("yyyy/MM/dd HH:mm:ss");
                else
                    return dt.ToString("yyyy/MM/dd HH:mm:ss.fff");
            }
            else
            {
                if (dt.Millisecond == 0 && dt.Second == 0)
                    return dt.ToString("HH:mm");
                else if (dt.Millisecond == 0)
                    return dt.ToString("HH:mm:ss");
                else
                    return dt.ToString("HH:mm:ss.fff");
            }
        }

        /// <summary>
        /// Take an int value ie 7 and split to a collection of ints that are powers of 2 that sum to this value ie 1,2,4
        /// </summary>
        /// <param name="compositeRights">Int32; Integer</param>
        /// <returns>List of type Int32; Ints that sum to the target value</returns>
        /// <remarks>
        /// Used where a composite rights value must be broken out into it's constituent rights.
        /// </remarks>
        public static List<int> SplitRightsInt(int compositeRights)
        {
            List<int> results = new List<int>();

            if (compositeRights <= 0)
                return results;

            int i = 0;
            for (; i < 32; i++)
            {
                int mask = 1 << i;
                if ((compositeRights & mask) != 0)
                {
                    results.Add(mask);
                }
            }

            return results;
        }


        public static string Int64ToBase36String(Int64 number)
        {
            StringBuilder sb = new StringBuilder();
            Int64 radix = 36;

            do
            {
                Int64 remainder = number % radix;
                number = number / radix;
                if (remainder <= 9)
                {
                    sb.Append((char)(remainder + 48)); // '0' = 48
                }
                else
                {
                    sb.Append((char)(remainder + 55)); // 'A'(65) - 10 = 55
                }
            }
            while (number > 0);

            return sb.ToString();
        }

        public static string GenerateID(Int64 seq)
        {
            DateTime dtSeed = new DateTime(2015, 01, 01);
            Int64 tickDelta = (DateTime.UtcNow.Ticks - dtSeed.Ticks) / 10000000; // 10000 ticks in a millisecond, 1000 millis in a second, so this should be 'seconds since epoch'
            string code = AppUtility.Int64ToBase36String(tickDelta);
            string id = code + "-" + (seq).ToString();
            return id;
        }

        /// <summary>
        /// Strip illegal chars and reserved words from a candidate filename (should not include the directory path)
        /// </summary>
        /// <remarks>
        /// http://stackoverflow.com/questions/309485/c-sharp-sanitize-file-name
        /// </remarks>
        public static string CoerceValidFileName(string filename)
        {
            var invalidChars = Regex.Escape(new string(Path.GetInvalidFileNameChars()));
            var invalidReStr = string.Format(@"[{0}]+", invalidChars);

            var reservedWords = new[]
                                    {
                                        "CON", "PRN", "AUX", "CLOCK$", "NUL", "COM0", "COM1", "COM2", "COM3", "COM4",
                                        "COM5", "COM6", "COM7", "COM8", "COM9", "LPT0", "LPT1", "LPT2", "LPT3", "LPT4",
                                        "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
                                    };

            var sanitisedNamePart = Regex.Replace(filename, invalidReStr, "_");
            foreach (var reservedWord in reservedWords)
            {
                var reservedWordPattern = string.Format("^{0}\\.", reservedWord);
                sanitisedNamePart = Regex.Replace(sanitisedNamePart, reservedWordPattern, "_RES_.", RegexOptions.IgnoreCase);
            }

            return sanitisedNamePart;
        }


    } // end of class AppUtility

} // end of namespace NZ01