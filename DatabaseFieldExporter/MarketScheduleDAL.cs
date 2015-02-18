using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Data.Common; // DbConnection
using System.Text; // StringBuilder

using System.Diagnostics; // StopWatch

namespace NZ01
{

    /// <summary>
    /// Class to access the Database's MarketSchedule table
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class MarketScheduleDAL : BaseDAL
    {
        ///////////////////
        // STATIC MEMBERS

        private static log4net.ILog logger = log4net.LogManager.GetLogger("MarketScheduleDAL");

        private static string _SAVEPATH = @"~/Schedules";
        private static string _EXT = ".txt";


        ///////////////
        // MEMBER FNS

        public Dictionary<Int64,Scheduler.Schedule> Select()
        {
            var prefix = "Select() - ";

            string sql = "SELECT * FROM MarketSchedule WHERE MSC_Deleted=0;";

            Dictionary<Int64, Scheduler.Schedule> dicReturn = new Dictionary<Int64, Scheduler.Schedule>();

            int countRecord = 0;
            try
            {
                logger.Info(prefix + string.Format("Issuing SQL command: >{0}<", sql));

                using (DbCommand cmd = DbAccess.DbUtils.CreateCmd(sql, GetConnection().Conn))
                {
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        ++countRecord;

                        string json = "";
                        Int64 pkey = BaseDAL.INVALIDPKEY;
                        Int64 seqNum = 0;

                        if (reader["MSC_PK"] != DBNull.Value) pkey = (Int64)reader["MSC_PK"];
                        if (reader["MSC_SeqNum"] != DBNull.Value) seqNum = (Int64)reader["MSC_SeqNum"];
                        if (reader["MSC_Schedule"] != DBNull.Value) json = (string)reader["MSC_Schedule"];

                        if (pkey == BaseDAL.INVALIDPKEY)
                        {
                            string msgInvalidPkey = "Invalid Pkey in MarketSchedule table.";
                            logger.Error(prefix + msgInvalidPkey);
                        }
                        else
                        {
                            if (string.IsNullOrWhiteSpace(json))
                            {
                                string msgEmptyJsonString = string.Format("Schedule in Json format was empty for pkey={0}.  Cannot create Schedule object.", pkey);
                                logger.Info(prefix + msgEmptyJsonString);
                            }
                            else
                            {
                                // Populate the dictionary
                                Scheduler.Schedule loadedSchedule = Scheduler.Schedule.FromJson(json);
                                dicReturn[pkey] = loadedSchedule;
                            }
                        }

                    }
                    reader.Close();

                } // end of using(cmd)
            }
            catch (Exception ex)
            {
                logger.Error(prefix + string.Format("Database Failure:{0}", ex.Message));
            }

            return dicReturn;
        }

        public bool UpdatePath(Int64 pkey, string virtualpath)
        {
            var prefix = "UpdatePath() - ";

            /////////////////////
            // Build SQL String

            StringBuilder sb = new StringBuilder();


            sb.Append(string.Format("UPDATE MarketSchedule SET MSC_UpdateTimestamp={0},", getDate()));
            string sqlVirtualPath = BaseDAL.sqlize(virtualpath,BaseDAL.STANDARD_FIELD_SIZE,true,true);
            sb.Append(string.Format("MSC_Schedule={0}",sqlVirtualPath));
            sb.Append(string.Format(" WHERE MSC_PK={0} AND MSC_Deleted=0;", pkey));

            return ExecNonQuery(sb.ToString(), logger, prefix);
        }

        public static string GetVirtualPath(string filename)
        {
            return _SAVEPATH + @"/" + filename + _EXT;
        }

        public static string GetVirtualPath(string filename, string savepath)
        {
            return savepath + @"/" + filename + _EXT;
        }

    } // end of class MarketScheduleDAL

} // end of namespace NZ01