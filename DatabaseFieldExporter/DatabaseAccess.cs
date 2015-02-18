using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using System.Data.Common;
using System.Configuration; // ConfigurationManager

using System.Data.Odbc;
using System.Data.SqlClient;
using System.Data.Sql;

using NZ01;

// Lifted wholesale (and then tweaked) from:
// https://sites.google.com/site/mrmbookmarks/msg/C---DbCommand-factory-to-completely-remove-the-dependency-to-any-specific-DB-Client


namespace DbAccess
{
    public static class DbUtils
    {
        private static log4net.ILog logger = log4net.LogManager.GetLogger("DBAccess.DBUtils");

        private static int _waitTimeoutDBConnection = 0;

        static DbUtils()
        {
            // STATIC CTOR
            AppUtility.LoadIntVariable(ref _waitTimeoutDBConnection, "DBAccess_DBUtils_waitTimeoutDBConnection");
        }

        public static DbCommand CreateCmd(string sqlQuery, DbConnection conn)
        { return CreateCmd(sqlQuery, conn, CommandType.Text); }                    // Wrapper on the function below
        
        public static DbCommand CreateCmd(string sqlQuery, DbConnection conn, CommandType commandType)
        {
            var prefix = "CreateCmd() - ";

            if (conn == null)
            {
                string msgNullConnection = string.Format("DBConnection object was null; Cannot create command object for SQL Query >{0}<", sqlQuery);
                logger.Info(prefix + msgNullConnection);

                return null;
            }

            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sqlQuery;
            cmd.CommandType = commandType;
            return cmd;
        }

        public static DbCommand CreateTransCmd(DbConnection conn, CommandType commandType = CommandType.Text)
        {
            if (conn == null)
                return null;            

            DbCommand cmd = conn.CreateCommand();
            cmd.CommandType = commandType;

            DbTransaction trans = conn.BeginTransaction();
            cmd.Transaction = trans;
            return cmd;
        }

        /// <summary>
        /// Create DbParameter using the configured DbFactory
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns>DbParameter</returns>
        public static DbParameter CreateParameter(string name, object value)
        {
            DbParameter param = DbProviderFactoryFactory.Create().CreateParameter();
            param.Value = value;
            param.ParameterName = name;

            return param;
        }

        
        public static void PruneSleepingMySQLConnections(Object dummy)
        {
            var prefix = "PruneSleepingMySQLConnections() - ";
            logger.Debug(prefix + "Entering");

            if (_waitTimeoutDBConnection <= 0)
            {
                logger.Debug(prefix + "Timeout set to zero (off);  No Action taken; Exiting");
                return;
            }

            string connectionStr = ConfigurationManager.ConnectionStrings["DbConnectionProvider"].ConnectionString;
            string dbType = ConfigurationManager.ConnectionStrings["DBType"].ConnectionString;

            if (dbType == "MYSQL")
            {
                logger.Debug(prefix + "MySQL detected.  Will action MySQL sleeping connection pruning...");
                int killedConnectionCount = KillSleepingConnections(_waitTimeoutDBConnection, connectionStr);
                logger.Debug(prefix + string.Format("Attempted to prune {0} connections", killedConnectionCount));
            }
            
            logger.Debug(prefix + "Exiting");
        }
        


        static public int KillSleepingConnections(int iMinSecondsToExpire,string connStr)
        {
            var prefix = string.Format("KillSleepingConnections(iMinSecondsToExpire={0}) - ", iMinSecondsToExpire);
            logger.Debug(prefix + "Entering");

            string sql = "show processlist";

            List<int> processesToKill = new List<int>();

            int countRecord = 0;
            DbAccess.WrappedConnection wrappedconn = new DbAccess.WrappedConnection(connStr);
            DbDataReader reader = null;
            try
            {
                logger.Debug(prefix + string.Format("Issuing SQL command: >{0}<", sql));

                using (DbCommand cmdSelect = DbAccess.DbUtils.CreateCmd(sql, wrappedconn.Conn))
                {
                    reader = cmdSelect.ExecuteReader();
                    while (reader.Read())
                    {
                        ++countRecord;

                        string pid = reader["Id"].ToString();
                        string state = reader["Command"].ToString();
                        string time = reader["Time"].ToString();

                        logger.Debug(prefix + string.Format("Connection: pid={0}, state={1}, time={2}", pid, state, time));

                        bool successPID = false;
                        bool successTime = false;

                        int iPID = 0;
                        int iTime = 0;
                        successPID = Int32.TryParse(pid, out iPID);
                        successTime = Int32.TryParse(time, out iTime);
                        
                        if (successPID && successTime && state == "Sleep" && iTime >= iMinSecondsToExpire && iPID > 0)
                        {
                            // This connection is sitting around doing nothing. Kill it.
                            processesToKill.Add(iPID);
                        }

                    } // end of "while (reader.Read())"

                    reader.Close();
                
                } // end of "using(DBCommand ...)"

                // Record the number of sleeping connections
                if (processesToKill.Count < 50)
                {
                    logger.Debug(prefix + string.Format("Sleeping connection count: ", processesToKill.Count));
                }
                else
                {
                    logger.Warn(prefix + string.Format("Sleeping connection count: ", processesToKill.Count));
                }

                foreach(int aPID in processesToKill)
                {
                    sql = "kill " + aPID;
                    using(DbCommand cmdKill = DbAccess.DbUtils.CreateCmd(sql, wrappedconn.Conn))
                    {
                        logger.Debug(prefix + string.Format("Issuing SQL command: >{0}<", sql));
                        cmdKill.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("Unknown thread id"))
                {
                    // Sleeping connections may be closed between the audit and the action, resulting in
                    // an attempt to kill a connection that no longer exists.  The failure is benign, and 
                    // just recorded for information.
                    
                    // IGNORE - Not an error
                    // logger.Debug(prefix + string.Format("Failed to kill sleeping connection; Exception Message: {0}", ex.Message));
                }
                else
                {
                    logger.Error(prefix + string.Format("Database Failure:{0}", ex.Message));
                }
            }
            finally
            {
                if (reader != null && !reader.IsClosed)
                {
                    reader.Close();
                }
            }

            logger.Debug(prefix + "Exiting");
            return processesToKill.Count;             
        }

        /// <summary>
        /// Factory to create DbConnections
        /// Connections should be disposed of via a 'using' block, or used in the WrappedConnection class
        /// </summary>
        public static class ConnectionFactory
        {
            public static DbConnection CreateConn(string connStr)
            {
                DbProviderFactory factory = DbProviderFactoryFactory.Create();
                
                DbConnection conn = factory.CreateConnection();
                conn.ConnectionString = connStr;

                return conn;
            }

        } // end of class ConnectionFactory



        /// <summary>
        /// Factory to create DbDataAdapter objects
        /// DbDataAdapters need properties setting:
        /// SelectCommand,InsertCommand,UpdateCommand,DeleteCommand.
        /// These properties are DbCommand objects, and can be obtained using DbUtils.
        /// DbCommand objects require a DbConnection object, which is opened and closed
        /// by the Fill() and Update() commands.
        /// </summary>
        public static class DataAdapterFactory
        {
            public static DbDataAdapter CreateDataAdapter(string sqlQuery,string connStr)
            {
                DbProviderFactory factory = DbProviderFactoryFactory.Create();
                DbDataAdapter adapter = factory.CreateDataAdapter();

                DbConnection conn = DbUtils.ConnectionFactory.CreateConn(connStr);

                adapter.SelectCommand = DbUtils.CreateCmd(sqlQuery, conn);             

                return adapter;
            }

        } // end of class DataAdapterFactory


        /// <summary>
        /// Singleton DbProviderFactory
        /// Instantiated from DatabaseAccess.ConnectionFactory.Create, which makes the connection
        /// </summary>
        private static class DbProviderFactoryFactory
        {
            // MEMBERS
            private static DbProviderFactory factory = null;

            // METHODS
            public static DbProviderFactory Create()
            {
                if (factory == null)
                {
                    // http://msdn.microsoft.com/en-us/library/dd0w4a2z.aspx

                    // This configuration is needed if you are going to use SqlClient.
                    // If you want to use any other client like MySql then you need to modify this setting.
                    //<appSettings>
                    //  <add key="DbConnFactory" value="System.Data.SqlClient" />
                    //</appSettings>

                    factory = DbProviderFactories.GetFactory(System.Configuration.ConfigurationManager.AppSettings["DbConnFactory"]);
                }

                return factory;

            } // end of Create()

        } // end of class DbProviderFactoryFactory

    } // end of class DbUtils



    /// <summary>
    /// A class to dispose of connections either manually or automatically (see Troelsen Ch8 p311)
    /// </summary>
    public class WrappedConnection : IDisposable
    {
        ///////////////////
        // STATIC MEMBERS

        private static log4net.ILog logger = log4net.LogManager.GetLogger("WrappedConnection");
        private static Int64 count = 0;


        // MEMBERS
        private bool bDisposed = false;
        private DbConnection conn = null;


        // CTOR/DTOR
        public WrappedConnection(string connString)
        {
            conn = DbAccess.DbUtils.ConnectionFactory.CreateConn(connString);

            if (conn != null)
            {
                conn.Open();
                ++count;
            }
        }

        ~WrappedConnection()
        {
            cleanUp(false);
        }

        // PROPERTIES
        public DbConnection Conn
        {
            get { return conn; }
        }

        // METHODS
        public void Dispose()
        {
            // User triggers a manual cleanup
            cleanUp(true);
            GC.SuppressFinalize(this);
        }

        public bool IsOpen()
        {
            if (conn != null)
            {
                return (conn.State == ConnectionState.Open);
            }
            else
                return false;
        }


        // HELPERS
        private void cleanUp(bool bDisposing)
        {
            var prefix = string.Format("cleanUp(bDisposing={0}) - ", bDisposing);

            if ( !this.bDisposed )
            {
                if ( bDisposing )
                {
                    if ( conn != null )
                    {                      
                        try
                        {
                            conn.Close();
                        }
                        catch (Exception ex)
                        {
                            logger.Warn(prefix + string.Format("Failed to close database connection: {0}",ex.Message));
                        }
                    }
                }
            }

            --count;
            bDisposed = true;
        }

    } // end of class WrappedConnection

} // end of namespace DbAccess
