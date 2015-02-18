using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;

using NZ01;

namespace DatabaseFieldExporter
{
    class Program
    {
        private static log4net.ILog logger = log4net.LogManager.GetLogger("Program");

        static void Main(string[] args)
        {
            var prefix = "Main() - ";

            ///////////////////////////////////////////////////////////////////
            // Start LOG4NET
            log4net.Config.XmlConfigurator.Configure();

            logger.Debug(prefix + "Entering.");
            Console.WriteLine("Entering...");

            exportSchedules();
            exportPositionLimits();

            Console.WriteLine("Press RETURN to exit...");
            Console.ReadLine();

            logger.Debug(prefix + "Exiting.");
        }

        private static void exportPositionLimits()
        {
            var prefix = "exportPositionLimits() - ";
            logger.Debug(prefix + "Entering");

            PositionLimitDAL plDAL = new PositionLimitDAL();
            Dictionary<Int64, PositionLimit> dicPLs = plDAL.SelectAllPositionLimits();

            // Create a PositionLimits directory
            string rootDir = @"C:/Temp/PositionLimits";
            System.IO.Directory.CreateDirectory(rootDir);

            Int64 seq = 0;
            foreach (KeyValuePair<Int64, PositionLimit> kvp in dicPLs)
            {
                ++seq;
                Int64 pkey = kvp.Key;
                PositionLimit pl = kvp.Value;

                StringUpper filename = pl.AccountKey.ToString() + "-" + AppUtility.GenerateID(seq);
                string virtualpath = PositionLimitDAL.GetVirtualPath(filename);
                string temppath = PositionLimitDAL.GetVirtualPath(filename, rootDir);
                pl.VirtualPath = virtualpath;

                // Write out to file
                System.IO.File.WriteAllText(temppath, pl.XmlLimitSpec.ToString(), Encoding.Unicode);

                // Update the path
                plDAL.UpdatePath(pl);
            }

            logger.Debug(prefix + "Exiting.");        
        }

        private static void exportSchedules()
        {
            var prefix = "exportSchedules() - ";
            logger.Debug(prefix + "Entering");

            MarketScheduleDAL msDAL = new MarketScheduleDAL();
            Dictionary<Int64, Scheduler.Schedule> dicSchedules = msDAL.Select();

            // Create a Schedules directory
            string rootDir = @"C:/Temp/Schedules";
            System.IO.Directory.CreateDirectory(rootDir);

            Int64 seq = 0;
            foreach (KeyValuePair<Int64, Scheduler.Schedule> kvp in dicSchedules)
            {
                ++seq;
                Int64 pkey = kvp.Key;
                Scheduler.Schedule schd = kvp.Value;
            
                // Create filename
                StringUpper filename = AppUtility.CoerceValidFileName(schd.Name.Truncate(32, false)) + "-" + AppUtility.GenerateID(seq);
                string virtualpath = MarketScheduleDAL.GetVirtualPath(filename);
                string temppath = MarketScheduleDAL.GetVirtualPath(filename, rootDir);

                // Write out to file
                System.IO.File.WriteAllText(temppath, Scheduler.Schedule.ToJson(schd), Encoding.Unicode);

                // Update the database
                msDAL.UpdatePath(pkey, virtualpath);
            }

            logger.Debug(prefix + "Exiting.");
        }
    }
}
