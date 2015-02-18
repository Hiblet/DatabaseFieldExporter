using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Data.Common; // DbConnection
using System.Text; // StringBuilder

namespace NZ01
{

    /// <summary>
    /// Class to access the Database's PositionLimit table 
    /// </summary>
    public class PositionLimitDAL : BaseDAL
    {
        ///////////////////
        // STATIC MEMBERS

        private static log4net.ILog logger = log4net.LogManager.GetLogger("PositionLimitDAL");

        private static string _SAVEPATH = @"~/PositionLimits";
        private static string _EXT = ".xml";


        //////////
        // CTORS

        public PositionLimitDAL()
        {
            // INSTANCE CTOR
        }



        ///////////////
        // MEMBER FNS


        public bool UpdatePath(PositionLimit pl)
        {
            var prefix = "UpdatePath() - ";

            /////////////////////
            // Build SQL String

            StringBuilder sb = new StringBuilder();

            sb.Append(string.Format("UPDATE POSITIONLIMIT SET PLM_LockNum=PLM_LockNum+1,PLM_UpdateTimestamp={0},", getDate()));
            string sqlVirtualPath = BaseDAL.sqlize(pl.VirtualPath,BaseDAL.STANDARD_FIELD_SIZE,true,true);
            sb.Append(string.Format("PLM_XmlLimitSpec={0},", sqlVirtualPath));
            sb.Append(string.Format("PLM_DefaultLimitMgmt={0},", pl.sqlDefaultLimitMgmt()));
            sb.Append(string.Format("PLM_SeqNum={0}", pl.SeqNum().ToString()));
            sb.Append(string.Format(" WHERE PLM_Account_FK={0} AND PLM_Deleted=0;", pl.sqlAccountKey()));

            return ExecNonQuery(sb.ToString(), logger, prefix);
        }


        /// <summary>
        /// Get all the position limits from the database
        /// </summary>
        /// <returns>Dictionary of Int64 (AccountKey) to PositionLimit object</returns>
        /// <remarks>Always selects the record with the highest sequence number if there are dupes.</remarks>
        public Dictionary<Int64, PositionLimit> SelectAllPositionLimits()
        {
            var prefix = "SelectAllPositionLimits() - ";

            Dictionary<Int64, PositionLimit> pls = new Dictionary<Int64, PositionLimit>();

            string sql = "SELECT * FROM POSITIONLIMIT WHERE PLM_Deleted=0;";

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

                        Int64 accountKey = -1;
                        string xmlLimitSpec = "";
                        string defaultLimitMgmt = "";
                        Int64 seqNum = 0;

                        if (reader["PLM_Account_FK"] != DBNull.Value) accountKey = (Int64)reader["PLM_Account_FK"];
                        if (reader["PLM_XmlLimitSpec"] != DBNull.Value) xmlLimitSpec = (string)reader["PLM_XmlLimitSpec"];
                        if (reader["PLM_DefaultLimitMgmt"] != DBNull.Value) defaultLimitMgmt = (string)reader["PLM_DefaultLimitMgmt"];
                        if (reader["PLM_SeqNum"] != DBNull.Value) seqNum = (Int64)reader["PLM_SeqNum"];

                        if (accountKey != -1 && seqNum != 0)
                        {
                            PositionLimit pl = new PositionLimit(accountKey, xmlLimitSpec, defaultLimitMgmt, "", seqNum);
                            AddDefensive(pls, pl);
                        }
                    }
                    reader.Close();

                } // end of using(cmd)
            }
            catch (Exception ex)
            {
                logger.Error(prefix + string.Format("Database Failure:{0}", ex.Message));
            }

            return pls;
        }



        ////////////
        // HELPERS

        private void AddDefensive(Dictionary<Int64, PositionLimit> pls, PositionLimit pl)
        {
            var prefix = "AddDefensive() - ";

            if (pls.ContainsKey(pl.AccountKey))
            {
                logger.Warn(prefix + string.Format("PositionLimit with ACCOUNTKEY=>{0}< already exists in the PositionLimit dictionary, ignoring this record.  Recommend removing records with duplicate account-symbol pairings from the PositionLimit table.", pl.AccountKey));

                PositionLimit plInMemory = pls[pl.AccountKey];
                if (pl.SeqNum() > plInMemory.SeqNum())
                {
                    pls[pl.AccountKey] = pl;
                    logger.Warn(prefix + string.Format("The database version of PositionLimit with ACCOUNTKEY=>{0}< has been loaded because it has a higher SeqNum ({1}) than the version in memory ({2}).", pl.AccountKey, pl.SeqNum(), plInMemory.SeqNum()));
                }
                else
                {
                    logger.Warn(prefix + string.Format("The database version of PositionLimit with ACCOUNTKEY=>{0}< has been ignored because it has an equal or lower SeqNum ({1}) than the version in memory ({2}).", pl.AccountKey, pl.SeqNum(), plInMemory.SeqNum()));
                }
            }
            else
            {
                // Does not already exist, add to dictionary
                pls.Add(pl.AccountKey, pl);
            }
        }

        public static string GetVirtualPath(string filename)
        {
            return _SAVEPATH + @"/" + filename + _EXT;
        }

        public static string GetVirtualPath(string filename, string savepath)
        {
            return savepath + @"/" + filename + _EXT;
        }


    } // end of class PositionLimitDAL

} // end of namespace NZ01