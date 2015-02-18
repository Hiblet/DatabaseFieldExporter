using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Data.Common; // DbConnection
using System.Configuration; // ConfigurationManager

using System.Diagnostics; // StopWatch

namespace NZ01
{
    /// <summary>
    /// Base class for all DAL's
    /// </summary>
    public class BaseDAL
    {
        ///////////////////
        // STATIC MEMBERS

        private static log4net.ILog logger = log4net.LogManager.GetLogger("BaseDAL");

        public static int STANDARD_FIELD_SIZE = 255; 
        public static string datetimeFormat = "yyyy-MM-ddTHH:mm:ss.fff";
        public static string dateOnlyFormat = "yyyy-MM-dd";
        public static string datetimeFormatNoMillis = "yyyy-MM-ddTHH:mm:ss";
        public static string connectionStr;
        public static string unicodePrefix;

        public static bool bUseQuotedDates = true;

        public static Int64 INVALIDPKEY = -1;

        public static int queryPerformanceWarningLimit = 5000;


        /////////////////////
        // INSTANCE MEMBERS

        private DbAccess.WrappedConnection _wrappedconn = null;



        ////////////////////
        // CTORS AND DTORS    

        static BaseDAL()
        {
            // STATIC CTOR
            connectionStr = ConfigurationManager.ConnectionStrings["DbConnectionProvider"].ConnectionString;
            unicodePrefix = "N";
            AppUtility.LoadIntVariable(ref queryPerformanceWarningLimit, "QueryPerformanceWarningLimit");
        }

        public BaseDAL()
        {
            var prefix = "BaseDAL() [ctor] - ";
            // INSTANCE CTOR

            // Instantiation implies creating and holding a connection open to the database
            try
            {
                _wrappedconn = new DbAccess.WrappedConnection(connectionStr);
            }
            catch (Exception ex)
            {
                //MarketManager.IsRunning(false);
                logger.Error(prefix + string.Format("Database Failure:{0}", ex.Message));
                //MarketManager.RaisedError(true);
            }
        }



        /////////////////////
        // MEMBER FUNCTIONS    

        public bool IsOpen()
        {
            if (_wrappedconn != null)
            {
                return _wrappedconn.IsOpen();
            }

            return false;
        }

        public DbAccess.WrappedConnection GetConnection()
        {
            return _wrappedconn;
        }

        public bool ExecNonQuery(string sql, log4net.ILog classLogger, string prefix)
        {
            //if (MarketManager.IsNotRunning("ExecNonQuery() " + sql))
            //    return false;

            if (GetConnection().Conn == null)
                return false;

            classLogger.Info(prefix + string.Format("Issuing SQL command: >{0}<", sql));

            Stopwatch stopwatch = new Stopwatch();

            try
            {
                using (DbCommand cmd = DbAccess.DbUtils.CreateCmd(sql, GetConnection().Conn))
                {
                    stopwatch.Start();
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                //MarketManager.IsRunning(false);
                classLogger.Error(prefix + string.Format("Database Failure:{0}", ex.Message));
                //MarketManager.RaisedError(true);
                return false;
            }
            finally
            {
                stopwatch.Stop();
                if (stopwatch.ElapsedMilliseconds > queryPerformanceWarningLimit)
                {
                    string msg = string.Format("Query elapsed time {0}ms exceeded warning limit {1}ms; SQL: >{2}<", stopwatch.ElapsedMilliseconds, queryPerformanceWarningLimit, sql);
                    classLogger.Warn(prefix + msg);
                }

            }

            return true;
        }


        /// <summary>
        /// Execute a collection of SQL non-query commands as a transaction
        /// </summary>
        /// <param name="sqlNonQueries">IEnumerable of type string; SQL commands to execute</param>
        /// <param name="classLogger">log4net.ILog; logger object passed through from the calling class</param>
        /// <param name="prefix">string; calling function name</param>
        /// <returns></returns>
        public bool ExecNonQueryTransaction(IEnumerable<string> sqlNonQueries, log4net.ILog classLogger, string prefix)
        {
            //if (MarketManager.IsNotRunning("ExecNonQueryTransaction() "))
            //    return false;

            if (GetConnection().Conn == null)
                return false;

            Stopwatch stopwatch = new Stopwatch();

            try
            {
                using (DbCommand cmd = DbAccess.DbUtils.CreateTransCmd(GetConnection().Conn))
                {
                    if (cmd != null)
                    {
                        try
                        {
                            // Loop the NonQuery sql strings attempting to execute them
                            int i = 0;
                            int length = sqlNonQueries.Count();
                            stopwatch.Start();
                            foreach (string sql in sqlNonQueries)
                            {
                                ++i;
                                cmd.CommandText = sql;

                                string msg = string.Format("Issuing SQL command ({0} of {1}): >{2}<", i, length, sql);
                                classLogger.Info(prefix + msg);

                                cmd.ExecuteNonQuery();
                            }

                            // Execute as a transaction
                            cmd.Transaction.Commit();
                        }
                        catch (Exception ex1)
                        {
                            string msg1 = string.Format("Transaction failed; Rollback will be attempted; Error:{0}", ex1.Message);
                            classLogger.Error(prefix + msg1);

                            try
                            {
                                cmd.Transaction.Rollback();
                            }
                            catch (Exception ex2)
                            {
                                string msg2 = string.Format("Transaction rollback failed, transaction was not active; Error:{0}", ex2.Message);
                                classLogger.Error(prefix + msg2);
                                return false;
                            }

                            return false;
                        }
                        finally
                        {
                            stopwatch.Stop();
                            if (stopwatch.ElapsedMilliseconds > queryPerformanceWarningLimit)
                            {
                                string msg = string.Format("Transaction elapsed time {0}ms exceeded warning limit {1}ms;", stopwatch.ElapsedMilliseconds, queryPerformanceWarningLimit);
                                classLogger.Warn(prefix + msg);
                            }
                        }
                    }
                    else
                    {
                        string msg = "CreateTransCommand() function return a null command object.";
                        classLogger.Error(prefix + msg);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                //MarketManager.IsRunning(false);
                classLogger.Error(prefix + string.Format("Database Failure:{0}", ex.Message));
                //MarketManager.RaisedError(true);
                return false;
            }

            return true;
        }

        public static string getDate()
        {
            string quote = "";

            if (bUseQuotedDates)
            {
                quote = "'";
            }

            return quote + DateTime.UtcNow.ToString(BaseDAL.datetimeFormat) + quote;
        }



        ////////////
        // SQL FNS

        public static string sqlize(string s, int maxsize = -1, bool unicode = true, bool tolerateForwardSlash = false)
        {
            if (maxsize == -1) maxsize = STANDARD_FIELD_SIZE;
            if (string.IsNullOrWhiteSpace(s)) return "NULL";
            else return (unicode ? unicodePrefix : "") + "'" + (s.SqlSanitize(tolerateForwardSlash)).Truncate(maxsize, false) + "'";
        }

        public static string sqlizeNoSanitize(string s, int maxsize = -1, bool unicode = true)
        {
            if (maxsize == -1) maxsize = STANDARD_FIELD_SIZE;
            if (string.IsNullOrWhiteSpace(s)) return "NULL";
            else return (unicode ? unicodePrefix : "") + "'" + s.Truncate(maxsize, false) + "'";
        }


        public static int sqlize(bool flag)
        {
            return (flag ? 1 : 0);
        }

        public Int64 GetCountRows(string table, string tableDeletedColumn, log4net.ILog classLogger)
        {
            var prefix = "GetCountRows() - ";

            //if (MarketManager.IsNotRunning("GetCountRows() "))
            //    return -1;

            Int64 iReturn = -1;
            int countRecord = 0;
            string sResultColumn = "COUNTROWS";

            string sqlSelect = string.Format("SELECT COUNT(*) AS {0} FROM {1}", sResultColumn, table);

            if (!string.IsNullOrWhiteSpace(tableDeletedColumn))
            {
                sqlSelect += string.Format(" WHERE {0}=0", tableDeletedColumn);
            }

            sqlSelect += ";";

            try
            {
                classLogger.Info(prefix + string.Format("Issuing SQL command: >{0}<", sqlSelect));

                using (DbCommand cmdSelect = DbAccess.DbUtils.CreateCmd(sqlSelect, GetConnection().Conn))
                {
                    DbDataReader reader = cmdSelect.ExecuteReader();
                    while (reader.Read())
                    {
                        ++countRecord;

                        if (reader[sResultColumn] != DBNull.Value)
                        {
                            iReturn = Convert.ToInt64(reader[sResultColumn]);
                        }
                    }
                }

                if (countRecord != 1)
                {
                    string msgCountWrong = string.Format("Count of {0} table returned a non-unit number of rows: {1}", table, countRecord);
                    classLogger.Warn(prefix + msgCountWrong);
                }
            }
            catch (Exception ex)
            {
                //MarketManager.IsRunning(false);
                classLogger.Error(prefix + string.Format("Database Failure:{0}", ex.Message));
                //MarketManager.RaisedError(true);
            }

            return iReturn;
        }

    } // end of class BaseDAL

} // end of namespace NZ01