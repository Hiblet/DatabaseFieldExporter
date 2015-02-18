using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Xml.Linq; // XElement type
using System.Text; // Encoding
using System.Text.RegularExpressions; // Regex
using log4net; // Log4Net ILog
using log4net.Core; // Log4Net Level

namespace NZ01
{

    /// <summary>
    /// Extension methods
    /// </summary>
    /// <remarks>
    /// 
    /// </remarks>
    public static class Extensions
    {
        //////////////////////
        // STRING EXTENSIONS
        //



        /// <summary>
        /// Check if a string contains text; Defaults to ignoring case, but can be over-ridden
        /// </summary>
        /// <param name="source"></param>
        /// <param name="toCheck"></param>
        /// <param name="comp"></param>
        /// <returns></returns>
        public static bool ContainsCaseInsensitive(this string source, string toCheck, StringComparison comp = StringComparison.OrdinalIgnoreCase)
        {
            return source.IndexOf(toCheck, comp) >= 0;
        }


        /// <summary>
        /// Get a string to report if it is Numeric in bases Dec/Hex/Bin/Oct
        /// </summary>
        /// <param name="s">string</param>
        /// <returns>bool; true if numeric</returns>
        /// <remarks>
        /// This is taken from PHP's isNumeric function.
        /// Ref: http://php.net/manual/en/function.is-numeric.php
        /// Source: http://stackoverflow.com/questions/894263/how-to-identify-if-a-string-is-a-number (see JDB answer)
        /// </remarks>
        public static bool IsNumeric(this String s)
        {
            return numericRegex.IsMatch(s);
        }

        /// <summary>
        /// Regex for IsNumeric
        /// </summary>
        static readonly Regex numericRegex =
            new Regex("^(" +
            /*Hex*/ @"0x[0-9a-f]+" + "|" +
            /*Bin*/ @"0b[01]+" + "|" +
            /*Oct*/ @"0[0-7]*" + "|" +
            /*Dec*/ @"((?!0)|[-+]|(?=0+\.))(\d*\.)?\d+(e\d+)?" + ")$");


        /// <summary>
        /// Get a string to report if it is alphanumeric only, no spaces
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static bool IsAlphanumeric(this String s)
        { return alphanumericRegex.IsMatch(s); }

        static readonly Regex alphanumericRegex = new Regex("^[a-zA-Z0-9]*$");


        /// <summary>
        /// Get a string to report if it is alphanumeric only, but with certain allowed characters.
        /// </summary>
        /// <param name="s"></param>
        /// <param name="sValidChars">string; String of allowed non-alphanumeric characters</param>
        /// <returns></returns>
        public static bool IsAlphanumericPlus(this String s, string sValidChars)
        {
            string sAllowed = sAlphaUpper + sAlphaLower + sNumeric + sValidChars;
            bool containsInvalidChars = false;
            foreach (char c in s)
            {
                if (sAllowed.IndexOf(c) == -1)
                {
                    // c is not in sAllowed, therefore it is invalid
                    containsInvalidChars = true;
                    break;
                }
            }

            return !containsInvalidChars;
        }

        static readonly string sAlphaUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        static readonly string sAlphaLower = "abcdefghijklmnopqrstuvwxyz";
        static readonly string sNumeric = "0123456789";





        /// <summary>
        /// Get a string to tell you if it is ASCII or not.
        /// </summary>
        /// <param name="s"></param>
        /// <returns>bool</returns>
        /// <remarks>
        /// XML using in XML.Linq cannot handle Unicode.
        /// The elements in the Limit spec are symbols, which are system generated ASCII codes,
        /// at least at time of writing.  The file should only have Ascii elements, so this 
        /// fn can be used to police that rule.
        /// </remarks>
        public static bool IsAscii(this String s)
        {
            string sOut = Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(s));
            return (sOut == s);
        }

        /// <summary>
        /// Remove any characters from a string that are not in the allowedChars string
        /// </summary>
        /// <param name="s">string; Input string</param>
        /// <param name="allowedChars">string; All allowed characters</param>
        /// <returns>string; The input string devoid of any characters not in the allowedChars string</returns>
        /// <remarks>
        /// This is case sensitive, so if you want to allow chars 'a' and 'A' include both in the allowed chars string.
        /// </remarks>
        public static string Filter(this String s, string allowedChars)
        {
            string sFiltered = string.Empty;

            foreach (char c in s)
            {
                if (allowedChars.Contains(c))
                    sFiltered += c;
            }

            return sFiltered;
        }

        /// <summary>
        /// Truncate a string if it exceeds a given length, and indicate truncation by adding an elipsis
        /// </summary>
        /// <param name="s">string;</param>
        /// <param name="maxLength">int; The maximum allowed length of the string</param>
        /// <param name="ellipsis">bool; Flag to indicate if an ellipsis should be used to show truncation</param>
        /// <returns>string; Truncated string with optional ellipsis</returns>
        public static string Truncate(this String s, int maxLength, bool ellipsis)
        {
            if (null == s)
                return s;

            if (maxLength <= 4)
                return s;

            if (s.Length > maxLength)
            {
                if (ellipsis)                
                    return s.Substring(0, maxLength - 3) + "...";                
                else                
                    return s.Substring(0, maxLength);                
            }
            else
                return s;
        }

        /// <summary>
        /// Get the ASCII representation of a string
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ToAscii(this String s)
        {
            return Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(s));
        }


        // V1 - Fixed
        //public static string SqlSanitize(this String stringValue)
        //{
        //    if (null == stringValue)
        //        return stringValue;
        //
        //    return stringValue
        //                .regexReplace("-{2,}", "-")                 // transforms multiple --- in - use to comment in sql scripts
        //                .regexReplace(@"['*/]+", string.Empty)      // removes / and * used also to comment in sql scripts - Hibbert: added single quote
        //                .regexReplace(@"(;|\s)(exec|execute|select|insert|update|delete|create|alter|drop|rename|truncate|backup|restore|error)\s", string.Empty, RegexOptions.IgnoreCase);
        //}

        // V2 - Conditional
        public static string SqlSanitize(this String stringValue, bool bTolerateForwardSlash = false)
        {
            if (null == stringValue)
                return stringValue;

            stringValue = stringValue.regexReplace("-{2,}", "-")            // Transforms multiple hyphens into single hyphens, used to comment in sql scripts
                        .regexReplace(@"(;|\s)(exec|execute|select|insert|update|delete|create|alter|drop|rename|truncate|backup|restore|error)\s", string.Empty, RegexOptions.IgnoreCase);

            if (bTolerateForwardSlash)
                return stringValue.regexReplace(@"['*]+", string.Empty);    // Allow forward slash, remove asterisk and single quote
            else
                return stringValue.regexReplace(@"['*/]+", string.Empty);   // Remove / and * used also to comment in sql scripts - Hibbert: added single quote
        }

        private static string regexReplace(this string stringValue, string matchPattern, string toReplaceWith)
        {
            return Regex.Replace(stringValue, matchPattern, toReplaceWith);
        }

        private static string regexReplace(this string stringValue, string matchPattern, string toReplaceWith, RegexOptions regexOptions)
        {
            return Regex.Replace(stringValue, matchPattern, toReplaceWith, regexOptions);
        }




        ///////////////////////////
        // LINQ TO XML EXTENSIONS
        //

        /// <summary>
        /// Get an XElement to return it's XPath location.
        /// </summary>
        /// <param name="element">XElement</param>
        /// <param name="sDelim">Delimiting string to separate levels in output text</param>
        /// <returns>string; Path location of XElement eg "ROOT/Level1/Level2/MyNode"</returns>
        public static string Path(this XElement element, string sDelim = "/")
        {
            XElement tmp = element;
            string path = string.Empty;
            while (tmp != null)
            {
                path = sDelim + tmp.Name + path;
                tmp = tmp.Parent;
            }
            return path.Substring(1);
        }


        /// <summary>
        /// Get an XElement to return it's XPath location by attribute.
        /// </summary>
        /// <param name="element">XElement</param>
        /// <param name="sDelim">Delimiting string to separate levels in output text</param>
        /// <returns>string; Path location of XElement eg "ROOT/Level1/Level2/MyNode"</returns>
        public static string AttributePath(this XElement element, string attribute, string sDelim = "/")
        {
            XElement tmp = element;
            string path = string.Empty;
            while (tmp != null)
            {
                string sAttrValue = "NULL";

                var attributeValue = tmp.Attribute(attribute).Value;
                if (attributeValue != null)
                    sAttrValue = attributeValue.ToString();

                path = sDelim + sAttrValue + path;
                tmp = tmp.Parent;
            }
            return path.Substring(1);
        }




        ///////////////////////////////////////////////////////////////////////////
        // DECIMAL EXTENSIONS
        //

        public static string ToTrimmedString(this decimal target, bool bIntegerShowsAsPointZero = true)
        {
            string strValue = target.ToString(); //Get the stock string

            //If there is a decimal point present
            if (strValue.Contains("."))
            {
                //Remove all trailing zeros
                strValue = strValue.TrimEnd('0');

                if (bIntegerShowsAsPointZero)
                {
                    //If all we are left with at the end of the string is a decimal point
                    if (strValue.EndsWith(".")) //then remove add a single trailing zero
                        strValue += "0";
                }
                else
                {
                    //If all we are left with at the end of the string is a decimal point
                    if (strValue.EndsWith(".")) //then remove it
                        strValue = strValue.TrimEnd('.');
                }
            }

            return strValue;
        }




        ///////////////////////////////////////////////////////////////////////
        // LOG4NET EXTENSIONS
        //

        public static void Log(this ILog log, Level level, string message, Exception exception = null)
        {
            var logger = log.Logger;
            logger.Log(logger.GetType(), level, message, exception);
        }
    }

} // end of namespace NZ01