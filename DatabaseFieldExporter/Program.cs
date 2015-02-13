using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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




            Console.WriteLine("Press RETURN to exit...");
            Console.ReadLine();

            logger.Debug(prefix + "Exiting.");
        }
    }
}
