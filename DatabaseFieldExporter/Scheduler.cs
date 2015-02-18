using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Web.Script.Serialization; // JSON De/Serialisation (System.Web.Extensions.DLL)
using System.Globalization; // DateTimeStyles, CultureInfo



namespace NZ01
{

    /// <summary>
    /// Scheduler is the container class for all classes handling scheduling tasks.
    /// </summary>
    public class Scheduler
    {



        /// <summary>
        /// The Marshall object is an ordered collection of the next events for a group of Schedule
        /// objects that have been 'marshalled' together.
        /// </summary>
        /// <remarks>
        /// Marshall objects work in UTC.  Schedules can each have their own Timezone.
        /// </remarks>
        public class Marshall
        {
            ///////////////////
            // STATIC MEMBERS

            private static log4net.ILog logger = log4net.LogManager.GetLogger("Scheduler_Marshall");

            private static Int64 _instanceCount;
            private static Object _lockerStatic;

            public static string NameStem;




            /////////////////////
            // INSTANCE MEMBERS

            private static Object _locker = new Object();

            private string _name;

            // A time ordered queue of next events, with only one event per schedule.
            // Why store the Schedule reference?  
            // - Two schedules may have the same next event DateTime, so they need to be differentiated.
            // - We need to immediately ask the schedule for it's next event, so this removes a lookup requirement.
            private SortedList<Tuple2Key<DateTime, Schedule>, StateChange> _queue = new SortedList<Tuple2Key<DateTime, Schedule>, StateChange>(new ByTuple2KeyDateTimeScheduleAscending());

            // A registry of StateChanges.  This enables a lookup of the StateChanges time, which
            // removes the need to iterate the queue when removing an event.  This should also allow
            // us to ensure that there is only one event on the queue at any one time for each Schedule.
            private Dictionary<Schedule, StateChange> _registryOfSCs = new Dictionary<Schedule, StateChange>();

            // A lookup to find which market is using which targeted instance of a schedule.
            // Each market should only ever have one schedule in operation, so this should be
            // a one-to-one relationship.
            private Dictionary<StringUpper,Schedule> _symbolToTargetedSchedule = new Dictionary<StringUpper,Schedule>();


            // A lookup to find the schedule instances that share the same pkey.
            // A single schedule might be used to create several targeted instances.
            private DictionaryOfHashSets<Int64, Schedule> _pkeyToTargetedSchedules
                = new DictionaryOfHashSets<Int64, Schedule>();


            //////////
            // CTORS

            // Static
            static Marshall()
            {
                var prefix = "Marshall() [STATIC CTOR] - ";
                logger.Debug(prefix + "Entering");

                _lockerStatic = new Object();

                NameStem = "Marshall_";

                logger.Debug(prefix + "Exiting");
            }

            // Default
            public Marshall()
            {
                _name = getNextName();
            }

            // Specific
            public Marshall(string name)
            {
                _name = name;
            }



            //////////////
            // ACCESSORS

            public string Name
            {
                get { return _name; }
                set { _name = value; }
            }





            /////////////////////
            // MEMBER FUNCTIONS


            public string ToDiagnosticString()
            {
                string sReturn = string.Empty;

                lock (_locker)
                {
                    sReturn += _name;
                    sReturn += ":";
                    sReturn += Environment.NewLine;

                    foreach (KeyValuePair<Tuple2Key<DateTime, Schedule>, StateChange> kvp in _queue)
                    {
                        Tuple2Key<DateTime, Schedule> queueKey = kvp.Key;
                        DateTime dt = queueKey.First;
                        Schedule schedule = queueKey.Second;
                        StateChange sc = kvp.Value;

                        sReturn += sc.ToString();
                        sReturn += Environment.NewLine;
                    }

                }

                return sReturn;
            }



            /// <summary>
            /// Register this Marshall object with a Schedule, and get the Schedule's next event relative to now.
            /// </summary>
            /// <param name="schedule">Object of type Schedule;</param>
            public void Register(Schedule schedule)
            {
                Register(schedule, DateTime.UtcNow);
            }

            /// <summary>
            /// Register this Marshall object with a Schedule, and get the Schedule's next event relative to the supplied sample point.
            /// </summary>
            /// <param name="schedule">Object of type Schedule;</param>
            /// <param name="dtUtcSamplePoint">DateTime; Time at which the marshall is registering;  Usually 'now' unless testing</param>
            public void Register(Schedule schedule, DateTime dtUtcSamplePoint)
            {
                var prefix = "Register() - ";

                if (schedule == null)
                {
                    string msgNullSchedule = "A NULL Schedule object was passed as an argument.";
                    logger.Warn(prefix + msgNullSchedule);
                    return;
                }

                // Marshall only deals with targeted instances of schedules
                if (string.IsNullOrWhiteSpace(schedule.Target))
                {
                    string msgNullScheduleTarget = "A Schedule object was passed that had no target symbol.";
                    logger.Warn(prefix + msgNullScheduleTarget);
                    return;
                }

                string msgRegisterAttempt = string.Format("Registering Schedule where the Name={0} and Target={1}", schedule.Name, schedule.Target);
                logger.Info(prefix + msgRegisterAttempt);

                lock (_locker)
                {
                    // DIAGNOSTIC
                    dump(prefix + "Before registering...");
                    // END DIAGNOSTIC

                    if (schedule.Register(this))
                    {
                        _pkeyToTargetedSchedules.Add(schedule.Pkey, schedule);

                        _symbolToTargetedSchedule.Add(schedule.Target, schedule);
                        
                        getNext(dtUtcSamplePoint, schedule);
                    }

                    // DIAGNOSTIC
                    dump(prefix + "After registering...");
                    // END DIAGNOSTIC
                
                }
            }


            /// <summary>
            /// Drop the current stored StateChange and re-pull.
            /// </summary>
            /// <param name="schedule"></param>
            /// <remarks>
            /// Called by a Schedule, so that it can indicate that it has changed.
            /// </remarks>
            public void Update(Schedule schedule)
            {
                var prefix = string.Format("Update(Schedule.Name={0},Schedule.Pkey={1}) - ", schedule.Name, schedule.Pkey);
                
                string msgUpdating = "Schedule change beginning...";
                logger.Info(prefix + msgUpdating);

                lock (_locker)
                {
                    // DIAGNOSTIC
                    dump(prefix + "Before updating...");
                    // END DIAGNOSTIC

                    // Get all schedules with the same pkey
                    List<Schedule> schedules = _pkeyToTargetedSchedules.Get(schedule.Pkey);

                    // Refresh the schedule targeted instances
                    foreach (Schedule schdToReplace in schedules)
                    {
                        // Remove pending state change
                        dropPendingStateChange(schdToReplace);

                        // Create a new instance based on changed master schedule
                        Schedule replacementTargetedScheduleInstance = ScheduleManager.GetTargetedScheduleCopy(schedule.Pkey, schdToReplace.Target);

                        // Transfer over current read points
                        replacementTargetedScheduleInstance.ReadPointNext = schdToReplace.ReadPointNext;
                        replacementTargetedScheduleInstance.ReadPointPrev = schdToReplace.ReadPointPrev;

                        // Update lookups to point at new instance
                        _pkeyToTargetedSchedules.Remove(schdToReplace);
                        _pkeyToTargetedSchedules.Add(replacementTargetedScheduleInstance.Pkey, replacementTargetedScheduleInstance);

                        _symbolToTargetedSchedule.Remove(schdToReplace.Target);
                        _symbolToTargetedSchedule.Add(replacementTargetedScheduleInstance.Target, replacementTargetedScheduleInstance);

                        // Put the next state change on the queue based on the new instance
                        getNext(replacementTargetedScheduleInstance.ReadPointNext, replacementTargetedScheduleInstance);
                    }

                    // DIAGNOSTIC
                    dump(prefix + "After updating...");
                    // END DIAGNOSTIC
                }

                string msgUpdatingCompleted = "Schedule change completed.";
                logger.Info(prefix + msgUpdatingCompleted);
            }


            public bool Unregister(Int64 pkey, string symbol)
            {
                var prefix = string.Format("Unregister(pkey={0}, symbol={1}) - ", pkey, symbol);

                // Sanity
                if (pkey <= 0)
                {
                    logger.Warn(prefix + "An invalid pkey value was passed as an argument; Exiting, no op");
                    return false;
                }

                if (string.IsNullOrWhiteSpace(symbol))
                {
                    logger.Warn(prefix + "No symbol string was passed as an argument; Exiting, no op");
                    return false;
                }

                bool result = true;

                lock (_locker)
                {
                    // DIAGNOSTIC
                    dump(prefix + "Before unregistering...");
                    // END DIAGNOSTIC

                    Schedule schd = getRegisteredScheduleInstanceBySymbol(symbol);

                    if (schd == null)
                    {
                        string msgNoScheduleFound = "Failed to unregister schedule;  Schedule was not found in symbol lookup.";
                        logger.Error(prefix + msgNoScheduleFound);
                        return false;
                    }

                    // Else, schd found, not null.

                    if (dropPendingStateChange(schd))
                    {
                        string msgDropGood = "Attempt to drop schedule targeted instance pending state change succeeded.";
                        logger.Info(prefix + msgDropGood);
                    }
                    else
                    {
                        string msgDropBad = "Failed to drop schedule targeted instance pending state change; dropPendingStateChange() function returned false.";
                        logger.Warn(prefix + msgDropBad);
                    }

                    

                    Schedule schdTargetedInstance = null;

                    if (_symbolToTargetedSchedule.TryGetValue(symbol, out schdTargetedInstance))
                    {
                        ////////////////////////////////
                        // Found the targeted instance

                        // Remove this symbol from the symbol lookup
                        _symbolToTargetedSchedule.Remove(symbol);

                        // Remove this schedule targeted instance from the set of instances using this pkey
                        _pkeyToTargetedSchedules.Remove(schdTargetedInstance);
                    }
                    else
                    {
                        ///////////////////
                        // Failed to find

                        string msgFailedToFindTargetedInstance = "Could not find a targeted instance of a schedule for this symbol.";
                        logger.Error(prefix + msgFailedToFindTargetedInstance);
                        result = false;
                    }
                                    
                }

                // DIAGNOSTIC
                dump(prefix + "After unregistering...");
                // END DIAGNOSTIC

                return result;
            }



            /// <summary>
            /// Retrieve a StateChange if it's trigger time is in the past;
            /// </summary>
            /// <param name="dtUtcSamplePoint">
            /// DateTime; Get the next triggered event relative to this point in time; Usually 'now', unless testing.
            /// </param>
            /// <returns>
            /// Object of type StateChange; 
            /// If no StateChange has triggered, a StateChange object with a 
            /// state of Scheduler.StateChange.INVALID_STATE is returned.
            /// </returns>
            /// <remarks>
            /// Usage: Expected usage is that this will be called in a while loop 
            /// by the client code until an invalid StateChange is returned.
            /// </remarks>
            public StateChange GetTriggered(DateTime dtUtcSamplePoint)
            {
                StateChange scReturn = null;

                lock (_locker)
                {
                    if (_queue.Any())
                    {
                        KeyValuePair<Tuple2Key<DateTime, Schedule>, StateChange> kvp = _queue.First();
                        Tuple2Key<DateTime, Schedule> t2key = kvp.Key;
                        // t2key.First == DateTime; // trigger time of state change
                        // t2key.Second == Schedule; // The schedule
                        if (dtUtcSamplePoint >= t2key.First)
                        {
                            // The StateChange at the front of the queue has triggered.
                            // Extract the state change, update the queue.
                            scReturn = kvp.Value;
                            Schedule schd = t2key.Second;

                            // Drop the StateChange from containers
                            _queue.RemoveAt(0);

                            _registryOfSCs.Remove(schd);

                            // Do not remove the targeted schedule instance from
                            // the lookup containers, because the schedule instance
                            // is still valid and active.
                            // It is only the StateChange that is being removed,
                            // and replaced with a new state change.
                            
                            // Pull in the next state change
                            getNext(t2key.First, t2key.Second); // First=DateTime, Second=Schedule
                        }
                    }

                    return scReturn;
                }
            }

            /// <summary>
            /// Retrieve a StateChange if it's trigger time is in the past; 
            /// </summary>
            /// <returns>
            /// Object of type StateChange; 
            /// If no StateChange has triggered, a StateChange object with a 
            /// state of Scheduler.StateChange.INVALID_STATE is returned.
            /// </returns>
            /// <remarks>
            /// Overload of GetTriggered(DateTime) which supplied DateTime.UtcNow as a default parameter
            /// </remarks>
            public StateChange GetTriggered()
            {
                return GetTriggered(DateTime.UtcNow);
            }

            private Schedule getRegisteredScheduleInstanceBySymbol(string symbol)
            {
                Schedule schd = null;

                if (_symbolToTargetedSchedule.TryGetValue(symbol, out schd))
                {
                    return schd;
                }

                return schd;
            }


            /// <summary>
            /// Return symbols for markets using this schedule
            /// </summary>
            /// <param name="schedulePkey"></param>
            /// <returns></returns>
            public List<string> GetSymbolsUsingSchedule(Int64 schedulePkey)
            {
                var prefix = string.Format("GetSymbolsUsingSchedule(schedulePkey={0}) - ",schedulePkey);

                HashSet<string> symbols = new HashSet<string>();

                if (schedulePkey == BaseDAL.INVALIDPKEY || schedulePkey <= 0)
                {
                    string msgBadScheduleKey = "Schedule key was invalid.";
                    logger.Warn(prefix + msgBadScheduleKey);
                    return symbols.ToList();
                }

                List<Schedule> schedules = _pkeyToTargetedSchedules.Get(schedulePkey);

                foreach (Schedule schd in schedules)
                {
                    symbols.Add(schd.Target);
                }

                return symbols.ToList();
            }


            /// <summary>
            /// Remove StateChange for this Schedule
            /// </summary>
            /// <param name="schdTargetedInstance">Object of type Schedule;</param>
            private bool dropPendingStateChange(Schedule schdTargetedInstance)
            {
                var prefix = string.Format("dropPendingStateChange(schdTargetedInstance={0}) - ", schdTargetedInstance.ToString());

                StateChange sc = new StateChange();

                if (!_registryOfSCs.TryGetValue(schdTargetedInstance, out sc))
                {
                    string msgStateChangeNotFound = "A pending state change could not be found in _registryOfSCs for this schedule; This may be OK if the schedule is only made up of specific dates, but is not OK if the schedule has any repeating dates.";
                    logger.Warn(prefix + msgStateChangeNotFound);
                    return false;
                }

                // At This Point: The StateChange exists.
                bool result = false;

                // Remove from registry
                bool resultRemoveFromRegistry = _registryOfSCs.Remove(schdTargetedInstance);
                string msgResultRemoveFromRegistry = string.Format("Result of attempt to remove state change from _registryOfSCs: {0}",resultRemoveFromRegistry);
                logger.Debug(prefix + msgResultRemoveFromRegistry);

                // DIAGNOSTIC
                string msgKey = string.Format("Building t2key to remove SC from queue using sc.DateAndTime value [{0}] and schedule value [{1}]", sc.DateAndTime, schdTargetedInstance.ToString());
                logger.Debug(prefix + msgKey);
                // END DIAGNOSTIC

                // Remove from queue
                Tuple2Key<DateTime, Schedule> t2key = new Tuple2Key<DateTime, Schedule>(sc.DateAndTime, schdTargetedInstance);
                bool resultRemoveFromQueue = _queue.Remove(t2key);
                string msgResultRemoveFromQueue = string.Format("Result of attempt to remove state change from _queue: {0}",resultRemoveFromQueue);
                logger.Debug(prefix + msgResultRemoveFromQueue);

                result = resultRemoveFromRegistry && resultRemoveFromQueue;

                string msgReturning = string.Format("Returning: {0}",result);
                logger.Debug(prefix + msgReturning);

                return result;
            }


            /// <summary>
            /// Request the next StateChange after a point in time from a Schedule
            /// </summary>
            /// <param name="dtLast">DateTime; Get the state change after this point in time</param>
            /// <param name="schedule">Object of type Schedule;</param>
            private bool getNext(DateTime dtLast, Schedule schedule)
            {
                var prefix = string.Format("getNext(dtLast={0},schedule={1},target={2}) - ", dtLast, schedule.Name, schedule.Target);

                // Check the registry;
                // There should be only one StateChange present per schedule, so fail if this schedule has a current entry.
                // If no entry exists in the registry, get the next StateChange.

                StateChange sc = new StateChange();

                if (_registryOfSCs.TryGetValue(schedule, out sc))
                {
                    // A StateChange already exists, do not get another
                    string msg = "WARNING: A new StateChange has been requested when one exists already; Not pulling next StateChange.";
                    logger.Warn(prefix + msg);
                    return false;
                }

                // At This Point: No StateChange currently exists

                sc = schedule.GetNext(dtLast);

                if (StateChange.IsValid(sc))
                {
                    // A StateChange has been returned.

                    // Create a key and add this StateChange to the queue at this key.
                    Tuple2Key<DateTime, Schedule> t2key = new Tuple2Key<DateTime, Schedule>(sc.DateAndTime, schedule);
                    _queue.Add(t2key, sc);

                    // Add to the registry
                    _registryOfSCs.Add(schedule, sc);
                }

                return true;
            }

            private static string getNextName()
            {
                lock (_lockerStatic)
                {
                    ++_instanceCount;
                    return NameStem + _instanceCount.ToString();
                }
            }

            private void dump(string text = "")
            {
                var prefix = "dump() - ";

                if (!string.IsNullOrWhiteSpace(text))                
                    prefix += string.Format("[{0}] - ",text);
                

                string msgStart = string.Format("***** START DUMP OF MARSHALL *****");
                logger.Debug(prefix + msgStart);

                lock (_locker)
                {
                    logger.Debug(prefix + "_name:" + _name);
                    
                    
                    logger.Debug(prefix + "*** START REGISTRYOFSCS ***");
                    int count1 = 0;
                    foreach (KeyValuePair<Schedule, StateChange> kvp1 in _registryOfSCs)
                    {
                        count1++;
                        Schedule schd = kvp1.Key;
                        StateChange sc = kvp1.Value;

                        string msgRegistryOfSCsRecord = string.Format("Item {0}: Schedule: {1}, StateChange: {2}", count1, schd.ToString(), sc.ToString());
                        logger.Debug(prefix + msgRegistryOfSCsRecord);
                    }
                    logger.Debug(prefix + "*** FINISH REGISTRYOFSCS ***");



                    logger.Debug(prefix + "*** START QUEUE ***");
                    int count2 = 0;
                    foreach(KeyValuePair<Tuple2Key<DateTime,Schedule>,StateChange> kvp2 in _queue)
                    {
                        count2++;
                        Tuple2Key<DateTime, Schedule> t2key = kvp2.Key;
                        StateChange sc = kvp2.Value;
                        
                        DateTime dt = t2key.First;
                        Schedule schd = t2key.Second;

                        string msgQueueRecord = string.Format("Item {0}: DateTime: {1}, Schedule: {2}, StateChange: {3}", count2, dt.ToString(BaseDAL.datetimeFormat), schd.ToString(), sc.ToString() );
                        logger.Debug(prefix + msgQueueRecord);
                    }
                    logger.Debug(prefix + "*** FINISH QUEUE ***");


                    // private Dictionary<string,Schedule> _symbolToTargetedSchedule = new Dictionary<string,Schedule>();
                    logger.Debug(prefix + "*** START SYMBOLTOTARGETEDSCHEDULE ***");
                    int count3 = 0;
                    foreach (KeyValuePair<StringUpper, Schedule> kvp3 in _symbolToTargetedSchedule)
                    {
                        count3++;
                        string symbol = kvp3.Key;
                        Schedule schd = kvp3.Value;

                        string msgSymbolToTargetedScheduleRecord = string.Format("Item {0}: Symbol: {1}, Schedule: {2}", count3, symbol, schd.ToString());
                        logger.Debug(prefix + msgSymbolToTargetedScheduleRecord);
                    }
                    logger.Debug(prefix + "*** FINISH SYMBOLTOTARGETEDSCHEDULE ***");


                    logger.Debug(prefix + "*** START PKEYTOTARGETEDSCHEDULES ***");
                    int count4 = 0;
                    foreach (Int64 schedulePkey in _pkeyToTargetedSchedules)
                    {
                        count4++;
                        List<Schedule> schedules = _pkeyToTargetedSchedules.Get(schedulePkey);
                        int count41 = 0;
                        foreach (Schedule schd in schedules)
                        {
                            count41++;

                            string msgPkeyToTargetedSchedulesRecord =
                                string.Format("Pkey Item {0}, Schedule Item {1}: SchedulePkey={2}, Schedule={3}", count4, count41, schedulePkey, schd.ToString());

                            logger.Debug(prefix + msgPkeyToTargetedSchedulesRecord);
                        }
                    }

                    logger.Debug(prefix + "*** FINISH PKEYTOTARGETEDSCHEDULES ***");
                }

                string msgFinish = string.Format("***** FINISH DUMP OF MARSHALL *****");
                logger.Debug(prefix + msgFinish);
            }

        } // end of class Marshall










        /// <summary>
        /// A manager to load, create and allow lookup between Pkey and Schedule object
        /// </summary>
        public class ScheduleManager
        {
            ///////////////////
            // STATIC MEMBERS

            private static log4net.ILog logger = log4net.LogManager.GetLogger("Scheduler_ScheduleManager");

            private static Object _locker;
            private static Int64 _seq = 0;
            private static DateTime _dtSeed = new DateTime(2015, 01, 01);

            private static Dictionary<Int64, Schedule> _schedulesByPKey;


            /////////
            // CTOR

            /// <summary>
            /// Static Ctor; Loads the schedules from the database at start
            /// </summary>
            static ScheduleManager()
            {
                var prefix = "ScheduleManager [STATIC CTOR] - ";
                logger.Debug(prefix + "Entering");

                _locker = new Object();
                _schedulesByPKey = new Dictionary<Int64, Schedule>();

                loadSchedules();

                logger.Debug(prefix + "Exiting");
            }



            /////////////////////
            // MEMBER FUNCTIONS


            /// <summary>
            /// Load schedules table from database
            /// </summary>
            private static void loadSchedules()
            {
                //MarketScheduleDAL mscDAL = new MarketScheduleDAL();
                //mscDAL.Select(_schedulesByPKey);
            }

            public static bool Exists(Int64 pkey)
            {
                lock (_locker)
                {
                    return _schedulesByPKey.ContainsKey(pkey);
                }
            }

            public static Schedule GetScheduleCopy(Int64 pkey)
            {
                Schedule schd = null;
                lock (_locker)
                {
                    if (_schedulesByPKey.TryGetValue(pkey, out schd))
                    {
                        // Schedule found.  Return a copy of it.
                        return new Schedule(schd);
                    }
                }

                return null;
            }

            public static string GetScheduleName(Int64 pkey)
            {
                Schedule schd = null;
                lock (_locker)
                {
                    if (_schedulesByPKey.TryGetValue(pkey, out schd))
                    {
                        // Schedule found.  Return a copy of it.
                        return schd.Name;
                    }
                }

                return string.Empty;
            }


            /// <summary>
            /// Get a dictionary detailing all held schedules.
            /// </summary>
            /// <returns>
            /// Dictionary of Int64 to Schedule;
            /// Value is the Schedule object;
            /// Key is the schedule pkey eg 123;
            /// Note: Returned dictionary is a copy.  References in dictionary will point to 
            ///       a Schedule that has it's own lock.
            /// </returns>
            public static Dictionary<Int64,Schedule> GetSchedules()
            {
                lock (_locker)
                {
                    // Return shallow copy to minimise lock time
                    return new Dictionary<Int64, Schedule>(_schedulesByPKey);
                }
            }


            /// <summary>
            /// Retrieve a Schedule from the Manager and set the target for the Schedule to this symbol.
            /// </summary>
            /// <param name="pkey">Int64; Schedule pkey</param>
            /// <param name="symbol">string; Symbol</param>
            /// <returns>
            /// If key does not exist, returns NULL;
            /// If key exists, returns ref to schedule with target set to symbol;
            /// </returns>
            /// <remarks>
            /// Schedules have targets that are used to link triggered events to relevant symbols.  
            /// </remarks>
            public static Schedule GetTargetedScheduleCopy(Int64 pkey, string symbol)
            {
                var prefix = string.Format("GetSchedule(pkey={0},symbol={1}) - ", pkey, symbol);

                Schedule schd = null;

                lock (_locker)
                {
                    if (_schedulesByPKey.TryGetValue(pkey, out schd))
                    {
                        // Schedule retrieved; 
                        // Copy, set target, and return copy

                        Schedule schdCopy = new Schedule(schd);
                        schdCopy.Target = symbol;

                        return schdCopy;
                    }
                    else
                    {
                        string msgScheduleNotFound = "Could not find a Schedule for this market.";
                        logger.Debug(prefix + msgScheduleNotFound);
                    }
                }

                return schd;
            }

            /// <summary>
            /// Create a Schedule from the provided Json String, add to the database, and return it's key.
            /// </summary>
            /// <param name="sJson">string; Json representation of a Schedule</param>
            /// <param name="err">string; Reference to a string to report error message</param>
            /// <returns>
            /// Int64; Pkey value; 
            /// On Failure, a Pkey value of BaseDAL.INVALIDPKEY is returned.
            /// On Success, the private key of the Schedule in the database is returned.
            /// </returns>
            /// 
            /*
            public static Int64 CreateSchedule(string sJson, ref string err)
            {
                var prefix = "CreateSchedule() - ";

                string msgDiag = string.Format("Passed in Json String: >{0}<", sJson);
                logger.Debug(prefix + msgDiag);

                Schedule scheduleVirgin = Schedule.FromJson(sJson);

                if (scheduleVirgin == null)
                {
                    err = "Failed to deserialize JSON data string;";
                    logger.Error(prefix + err);
                    return BaseDAL.INVALIDPKEY;
                }

                // Create a unique filename from datetime and seq, plus the name of the schedule.
                StringUpper filename = AppUtility.CoerceValidFileName(scheduleVirgin.Name.Truncate(32, false)) + "-" + generateID();
                scheduleVirgin.VirtualPath = MarketScheduleDAL.GetVirtualPath(filename);

                MarketScheduleDAL mscDAL = new MarketScheduleDAL();
                mscDAL.Insert(scheduleVirgin);

                Int64 pkey = mscDAL.GetCreatedPkey(scheduleVirgin);

                if (pkey == BaseDAL.INVALIDPKEY)
                {
                    err = "Failed to retrieve a valid key for this schedule; Possible duplicate or database error;";
                    logger.Error(prefix + err);
                    return BaseDAL.INVALIDPKEY;
                }
                else
                {
                    scheduleVirgin.Pkey = pkey;
                    lock (_locker)
                    {
                        _schedulesByPKey.Add(scheduleVirgin.Pkey, scheduleVirgin);
                    }
                }

                return pkey;
            }
            */

            private static string generateID()
            {
                lock (_locker)
                {
                    Int64 tickDelta = (DateTime.UtcNow.Ticks - _dtSeed.Ticks) / 10000000; // 10000 ticks in a millisecond, 1000 millis in a second, so this should be 'seconds since epoch'
                    string code = AppUtility.Int64ToBase36String(tickDelta);
                    string id = code + "-" + (++_seq).ToString();
                    return id;

                } // unlock
            }

        } // end of class ScheduleManager










        /// <summary>
        /// The Schedule object is an object that combines weekly and yearly repeating schedules with 
        /// specific dated events.  It can be queried for the next or previous event, relative to 
        /// a given timestamp.
        /// </summary>
        /// <remarks>
        /// TimeZones: 
        ///   Current implementation respects TimeZones for a Schedule, but NOT Daylight Saving Time (DST).
        ///   The reason is that DST changes cause ambiguous and skipped times to occur when converting
        ///   from local time to UTC time.  For example, 01:30 does not exist for a Northern Hemisphere 
        ///   Spring (forward) change, and for Autumn (backward) changes some local times exist twice.
        ///   The timezone's UTC base offset is used to manually shift between UTC and local time.
        /// 
        /// Target:
        ///   Each market should have it's own schedule, even if several markets point at the same 
        ///   schedule key.  
        ///   The target should be set at runtime and not persisted.  Effectively it is the backwards
        ///   lookup for the schedule key, so that when the schedule event fires, we know which market
        ///   the event is relevant to.
        /// </remarks>
        public class Schedule
        {


            ///////////////////
            // STATIC MEMBERS

            private static log4net.ILog logger = log4net.LogManager.GetLogger("Scheduler_Schedule");

            private static Int64 _instanceCount;
            private static Object _lockerStatic;

            // JSON property names
            public static string DefaultWSKey;
            public static string RepeatYearliesKey; // Collection property name
            public static string RepeatYearlyKey;
            public static string RepeatYearlyDSKey;
            public static string SpecificDatesKey; // Collection property name
            public static string SpecificDateKey;
            public static string SpecificDateDSKey;
            public static string NameKey;
            public static string TargetKey;
            public static string TimeZoneInfoKey;
            public static string DSLookupKey;
            public static string DailyScheduleNameKey;
            public static string DailyScheduleKey;


            public static string NameStem;


            /////////////////////
            // INSTANCE MEMBERS


            private string _name; // User-friendly name of the Schedule
            private StringUpper _target; // Symbol (market) that the schedule controls, not persisted
            private StringUpper _virtualpath; // Full path to storage file
            private Int64 _seqNum = 0;
            private Int64 _pkey = BaseDAL.INVALIDPKEY;

            private Object _locker = new Object();

            // When a schedule is updated, the 'next StateChange' call will have to be run
            // again, so that the Marshall is always in step with the Schedule.  To do this,
            // we have to cache the dateTime that the call uses, and re-issue the call 
            // with that timestamp when the schedule is changed.  This member variable is
            // the storage for that dateTime...
            private DateTime _dtUtcReadPoint_Next = DateTime.MinValue;
            private DateTime _dtUtcReadPoint_Prev = DateTime.MinValue;


            // Storage
            private WeeklySchedule _defaultWS = new WeeklySchedule();
            private SortedList<DateTime, string> _repeatYearlyDSs = new SortedList<DateTime, string>(new ByDTDateAscending());
            private SortedList<DateTime, string> _specificDateDSs = new SortedList<DateTime, string>(new ByDTDateAscending());

            // Cache
            private Dictionary<int, SortedList<DateTime, DailySchedule>> _cachedYearlyDSs = new Dictionary<int, SortedList<DateTime, DailySchedule>>();

            // Lookup
            private Dictionary<string, DailySchedule> _dsLookup = new Dictionary<string, DailySchedule>();

            // Schedules can be defined relative to TimeZones
            private TimeZoneInfo _tzi = TimeZoneInfo.Utc;

            // Collection of Marshalls that must be informed when the schedule changes.  Transient information, not stored.
            private List<Marshall> _marshalls = new List<Marshall>();




            //////////
            // CTORS

            // Static Ctor
            static Schedule()
            {
                _lockerStatic = new Object();

                NameStem = "Schd_";
                NameKey = "Name";
                TargetKey = "Target";
                TimeZoneInfoKey = "TZ";
                DefaultWSKey = "DefWkSchd";
                RepeatYearliesKey = "RepYearly";
                RepeatYearlyKey = "RepDate";
                RepeatYearlyDSKey = "RepDateDS";
                SpecificDatesKey = "Specific";
                SpecificDateKey = "SpecDate";
                SpecificDateDSKey = "SpecDateDS";
                DSLookupKey = "S_DSLookup";
                DailyScheduleKey = "DS";
                DailyScheduleNameKey = "DSName";
            }

            // Default
            public Schedule()
            {
                _name = getNextName();
                _seqNum = 1;
            }

            // Specific
            public Schedule(string name)
            {
                _name = name;
                _seqNum = 1;
            }

            // Copy
            public Schedule(Schedule schdOther)
            {
                if (schdOther != null)
                {
                    lock (schdOther._locker)
                    {
                        _name = schdOther._name;
                        _target = schdOther._target;
                        _defaultWS = new WeeklySchedule(schdOther._defaultWS);
                        _repeatYearlyDSs = new SortedList<DateTime, string>(schdOther._repeatYearlyDSs, new ByDTDateAscending());
                        _specificDateDSs = new SortedList<DateTime, string>(schdOther._specificDateDSs, new ByDTDateAscending());
                        _dsLookup = new Dictionary<string, DailySchedule>(schdOther._dsLookup);
                        _tzi = schdOther._tzi;

                        _pkey = schdOther._pkey;
                        _virtualpath = schdOther._virtualpath;
                        _seqNum = schdOther._seqNum;
                    }
                }
            }


            //////////////
            // ACCESSORS

            public string Name
            {
                get { lock (_locker) { return _name; } }
                set { lock (_locker) { _name = value; } }
            }

            public string Target
            {
                get { lock (_locker) { return _target; } }
                set { lock (_locker) { _target = value; } }
            }

            public string VirtualPath
            {
                get { lock (_locker) { return _virtualpath; } }
                set { lock (_locker) { _virtualpath = value; } }
            }

            public Int64 Pkey
            {
                get { lock (_locker) { return _pkey; } }
                set { lock (_locker) { _pkey = value; } }
            }

            public Int64 SeqNum
            {
                get { lock (_locker) { return _seqNum; } }
                set { lock (_locker) { _seqNum = value; } }
            }


            /// <summary>
            /// Directly get/set the TimeZoneInfo
            /// </summary>
            public TimeZoneInfo TimeZoneInfo
            {
                get { lock (_locker) { return _tzi; } }
                set
                {
                    lock (_locker)
                    {
                        _tzi = value;
                    }

                    onUpdate();
                }
            }

            /// <summary>
            /// Return a copy of the current weekly schedule
            /// </summary>
            /// <returns></returns>
            public WeeklySchedule GetWeeklyScheduleCopy()
            {
                lock (_locker)
                {
                    return new WeeklySchedule(_defaultWS);
                }
            }

            public SortedList<DateTime, string> GetRepeatYearlyDSsCopy()
            {
                lock (_locker)
                {
                    return new SortedList<DateTime, string>(_repeatYearlyDSs);
                }
            }

            public SortedList<DateTime, string> GetSpecificDateDSsCopy()
            {
                lock (_locker)
                {
                    return new SortedList<DateTime, string>(_specificDateDSs);
                }
            }


            public Dictionary<string, DailySchedule> GetDailySchedulesCopy()
            {
                Dictionary<string, DailySchedule> DSs = null;
                lock (_locker)
                {
                    DSs = _defaultWS.GetDailySchedulesCopy();
                    foreach (KeyValuePair<string, DailySchedule> kvp1 in _dsLookup)
                    {
                        // kvp1.Key = DailySchedule name 
                        // kvp1.Value = DailySchedule
                        DSs.Add(kvp1.Key, new DailySchedule(kvp1.Value));
                    }
                }

                return DSs;
            }

            /// <summary>
            /// The timestamp of the last requested state change
            /// </summary>
            /// <remarks>
            /// This is a record of the timestamp passed to the schedule, from which
            /// the next SC is requested.  The reason we need to keep this is that, 
            /// if the schedule is changed, the next SC may change, and in that case,
            /// a different SC should be loaded on the Marshall's queue.
            /// </remarks>
            public DateTime ReadPointNext
            {
                get { lock (_locker) { return _dtUtcReadPoint_Next; } }
                set { lock (_locker) { _dtUtcReadPoint_Next = value; } }
            }

            public DateTime ReadPointPrev
            {
                get { lock (_locker) { return _dtUtcReadPoint_Prev; } }
                set { lock (_locker) { _dtUtcReadPoint_Prev = value; } }
            }



            /////////////////////
            // MEMBER FUNCTIONS


            /// <summary>
            /// Set the TimeZoneInfo member from a TimeZoneInfo.Id string
            /// </summary>
            /// <param name="sTimeZoneInfoId">string; TimeZoneInfo Id property</param>
            /// <returns>
            /// string; 
            /// TimeZoneInfo.Id after change attempt;
            /// </returns>
            public string SetTimeZoneInfo(string sTimeZoneInfoId)
            {
                lock (_locker)
                {
                    _tzi = getTimeZoneInfoFromString(sTimeZoneInfoId);
                    _seqNum++;
                }

                onUpdate();

                return _tzi.Id;
            }


            public bool Register(Marshall marshall)
            {
                lock (_locker)
                {
                    if (!_marshalls.Contains(marshall))
                    {
                        _marshalls.Add(marshall);
                        return true;
                    }
                }

                return false;
            }


            public bool Unregister(Marshall marshall)
            {
                lock (_locker)
                {
                    if (_marshalls.Contains(marshall))
                    {
                        return _marshalls.Remove(marshall);
                    }
                }

                return false;
            }


            /// <summary>
            /// Set a default Weekly Schedule.
            /// </summary>
            /// <param name="ws">Object of type WeeklySchedule;</param>
            /// <returns>
            /// bool; True if schedule is set.
            /// </returns>
            /// <remarks>
            /// </remarks>
            public bool SetWeeklySchedule(WeeklySchedule ws)
            {
                lock (_locker)
                {
                    _defaultWS = ws;
                    _seqNum++;
                }

                onUpdate();

                return true;
            }

            /// <summary>
            /// Add a DailySchedule object to the default weekly schedule, overwriting any existent schedule.
            /// </summary>
            /// <param name="day">DayOfWeek enumeration; eg DayOfWeek.Monday</param>
            /// <param name="ds">Object of type DailySchedule;</param>
            public void AddDailySchedule(DayOfWeek day, DailySchedule ds)
            {
                lock (_locker)
                {
                    _defaultWS.Add(day, ds);
                    _seqNum++;
                }

                onUpdate();
            }

            /// <summary>
            /// Remove a DailySchedule from the default weekly schedule.
            /// </summary>
            /// <param name="day">DayOfWeek enumeration; eg DayOfWeek.Monday</param>
            /// <returns>bool; True if removed; Wrapper for Dictionary.Remove()</returns>
            public bool RemoveDailySchedule(DayOfWeek day)
            {
                bool bRemoved = false;

                lock (_locker)
                {
                    bRemoved = _defaultWS.Remove(day);
                    _seqNum++;
                }

                if (bRemoved)
                    onUpdate();

                return bRemoved;
            }


            /// <summary>
            /// Add a daily schedule for a specific, one-off date, overwriting any existing schedule.
            /// </summary>
            /// <param name="dt">DateTime; Date that the schedule should be used for; Time part is ignored.</param>
            /// <param name="ds">Object of type DailySchedule;</param>
            /// <remarks>
            /// </remarks>
            public void AddDailyScheduleForDate(DateTime dt, DailySchedule ds)
            {
                lock (_locker)
                {
                    _dsLookup[ds.Name] = ds;
                    _specificDateDSs[dt.Date] = ds.Name;
                    _seqNum++;
                }

                onUpdate();
            }

            /// <summary>
            /// Remove a daily schedule for a specific, one-off date.
            /// </summary>
            /// <param name="dt">DateTime; Date that the schedule should be deleted for; Time part is ignored.</param>
            /// <returns>
            /// bool; true if removed; Wrapper for List.Remove();
            /// </returns>
            public bool RemoveDailyScheduleForDate(DateTime dt)
            {
                bool bSuccess = false;

                lock (_locker)
                {
                    bSuccess = _specificDateDSs.Remove(dt.Date);
                    _seqNum++;
                }

                if (bSuccess)
                    onUpdate();

                return bSuccess;
            }


            /// <summary>
            /// Add a DailySchedule for a date that recurs each year.
            /// </summary>
            /// <param name="dt">DateTime; Date that the schedule should be used for; Year and Time part are ignored.</param>
            /// <param name="ds">Object of type DailySchedule;</param>
            public void AddDailyScheduleForRecurringDate(DateTime dt, DailySchedule ds)
            {
                lock (_locker)
                {
                    _cachedYearlyDSs.Clear();
                    _dsLookup[ds.Name] = ds;
                    _repeatYearlyDSs[dt.Date] = ds.Name;
                    _seqNum++;
                }

                onUpdate();
            }

            /// <summary>
            /// Remove a daily schedule for a date that recurs each year.
            /// </summary>
            /// <param name="dt">DateTime; Date that the schedule should be deleted for; Year and Time part are ignored.</param>
            public bool RemoveDailyScheduleForRecurringDate(DateTime dt)
            {
                bool bSuccess = false;

                lock (_locker)
                {
                    _cachedYearlyDSs.Clear();
                    bSuccess = _repeatYearlyDSs.Remove(dt.Date);
                    _seqNum++;
                }

                if (bSuccess)
                    onUpdate();

                return bSuccess;
            }


            /// <summary>
            /// A change has occurred; Trigger update of all observers.
            /// </summary>
            private void onUpdate()
            {
                List<Marshall> marshalls;

                // Lock object and copy the list of marshalls
                lock (_locker)
                {
                    marshalls = new List<Marshall>(_marshalls);
                }

                // Release the lock, and trigger updates in all Marshalls that refer to this schedule
                foreach (Marshall marshall in marshalls)
                {
                    marshall.Update(this);
                }
            }




            /// <summary>
            /// Get the next StateChange for this schedule, relative to the provided argument timestamp.
            /// </summary>
            /// <param name="dt">DateTime; Timestamp from which to base next StateChange search</param>
            /// <returns>Object of type StateChange; The next state change;</returns>
            /// <remarks>
            /// An instance of Marshall should be calling GetNext, with a UTC dateTime.
            /// </remarks>
            public StateChange GetNext(DateTime dt)
            {
                var prefix = "GetNext() - ";

                lock (_locker)
                {
                    _dtUtcReadPoint_Next = dt; // Store the timestamp of the request

                    // Convert the passed in DateTime to local Schedule timezone
                    DateTime dtLocalToSchedule = manualConvertTimeFromUtc(dt, _tzi);

                    // Get the StateChange
                    StateChange sc = getNextStateChange(dtLocalToSchedule);

                    if (StateChange.IsValid(sc))
                    {
                        // Convert the StateChange's committed DateTime to UTC
                        sc.DateAndTime = manualConvertTimeToUtc(sc.DateAndTime, _tzi);
                    }

                    // Stamp the origin on the StateChange
                    sc.ScheduleTarget = _target;

                    string msgSCSetup = string.Format("State Change [{0}] has been created for schedule with name={1} and target={2}", sc.ToString(), _name, _target);
                    logger.Debug(prefix + msgSCSetup);

                    return sc;
                }
            }


            public StateChange GetPrevious(DateTime dt)
            {
                lock (_locker)
                {
                    _dtUtcReadPoint_Prev = dt;

                    DateTime dtLocalToSchedule = manualConvertTimeFromUtc(dt, _tzi);

                    StateChange sc = getPreviousStateChange(dtLocalToSchedule);
                    if (StateChange.IsValid(sc))
                    {
                        sc.DateAndTime = manualConvertTimeToUtc(sc.DateAndTime, _tzi);
                    }

                    sc.ScheduleTarget = _target;

                    return sc;
                }
            }


            /// <summary>
            /// Return a DateTime that is offset by the amount dictacted by the supplied TimeZoneInfo object
            /// </summary>
            /// <param name="dtUtc">DateTime; UTC based dateTime object</param>
            /// <param name="_tzi">TimeZoneInfo; TZI of Schedule</param>
            /// <returns>
            /// DateTime manually shifted by the offset of the TimeZoneInfo
            /// </returns>
            /// <remarks>
            /// Why would we do this, and not use the actual value returned from TimeZoneInfo.ConvertTimeFromUtc()?
            /// The ConvertTimeFromUtc() function respects DST, which causes us to experience skipped times and
            /// ambiguous times.  This method uses the Schedule time as an offset within the day.
            /// The worst case scenario is that on DST days where the clock falls back, the day is 23 hours long,
            /// and StateChanges in the Schedule timezone in the range 23:00 to 24:00 are not triggered.
            /// </remarks>
            private DateTime manualConvertTimeFromUtc(DateTime dtUtc, TimeZoneInfo _tzi)
            {
                return dtUtc.Add(_tzi.BaseUtcOffset);
            }

            private DateTime manualConvertTimeToUtc(DateTime dtLocal, TimeZoneInfo _tzi)
            {
                return dtLocal.Subtract(_tzi.BaseUtcOffset);
            }


            private StateChange getNextStateChange(DateTime dt)
            {
                var prefix = string.Format("getNextStateChange(dt={0}) - ", dt.ToString(StateChange.UniversalDBFormat));

                StateChange scReturn = new StateChange();

                // Sanity shortcut: If there are no configured state changes, return immediately.
                if (_repeatYearlyDSs.Count() == 0 &&
                    hasNoSpecificDatesInFuture(dt) &&
                    _defaultWS.Count() == 0)
                    return scReturn;

                SortedList<DateTime, DailySchedule> allDSs = getAllDSs(dt);

                // We should now have one set of DailySchedules, ordered by Date, and 
                // prioritised Specific -> Repeating -> Weekly
                return getNextDatedSC(dt, allDSs);
            }

            private StateChange getPreviousStateChange(DateTime dt)
            {
                var prefix = string.Format("getPreviousStateChange(dt={0}) - ", dt.ToString(StateChange.UniversalDBFormat));

                StateChange scReturn = new StateChange();

                // Sanity shortcut: If there are no configured state changes, return immediately.
                if (_repeatYearlyDSs.Count() == 0 &&
                    hasNoSpecificDatesInPast(dt) &&
                    _defaultWS.Count() == 0)
                    return scReturn;

                SortedList<DateTime, DailySchedule> allDSs = getAllDSs(dt);

                // We should now have one set of DailySchedules, ordered by Date, and 
                // prioritised Specific -> Repeating -> Weekly
                return getPreviousDatedSC(dt, allDSs);
            }

            private SortedList<DateTime, DailySchedule> getAllDSs(DateTime dt)
            {
                // Create a single date-committed schedule based on the component schedules
                SortedList<DateTime, DailySchedule> allDSs = applyWeek(dt); // The committed weekly schedule for 3 weeks

                SortedList<DateTime, DailySchedule> committedYearlyDSs = getOrCreateYearlyDS(dt.Year);

                SortedList<DateTime, DailySchedule> committedSpecificDSs = getSpecificDS(); // Apply the DSLookup 

                // Overlay the repeating yearly schedule on top of the weekly schedule
                foreach (KeyValuePair<DateTime, DailySchedule> kvp in committedYearlyDSs)
                {
                    // DateTime dt = kvp.Key;
                    // DailySchedule ds = kvp.Value;
                    allDSs[kvp.Key] = kvp.Value;
                }

                // Overlay the specific date schedule on top of that
                foreach (KeyValuePair<DateTime, DailySchedule> kvp in committedSpecificDSs)
                {
                    // DateTime dt = kvp.Key;
                    // DailySchedule ds = kvp.Value;
                    allDSs[kvp.Key] = kvp.Value;
                }

                return allDSs;
            }




            ////////////
            // HELPERS




            private bool hasNoSpecificDatesInFuture(DateTime dt)
            {
                if (_specificDateDSs.Count() == 0)
                    return true;

                KeyValuePair<DateTime, string> kvpLast = _specificDateDSs.Last();

                DateTime dtLast = kvpLast.Key;

                return (dt.Date > dtLast);
            }


            private bool hasNoSpecificDatesInPast(DateTime dt)
            {
                if (_specificDateDSs.Count() == 0)
                    return true;

                KeyValuePair<DateTime, string> kvpFirst = _specificDateDSs.First();
                DateTime dtFirst = kvpFirst.Key;

                return (dt.Date < dtFirst);
            }


            private static string getNextName()
            {
                lock (_lockerStatic)
                {
                    ++_instanceCount;
                    return NameStem + _instanceCount.ToString();
                }
            }


            private DailySchedule getNextDatedDS(DateTime dt, SortedList<DateTime, DailySchedule> list)
            {
                DailySchedule ds;
                list.TryGetValue(dt.Date, out ds);
                return ds;
            }


            private StateChange getNextDatedSC(DateTime dt, SortedList<DateTime, DailySchedule> list)
            {
                DateTime dtSearch = dt; // DateTime is a value type - you get a copy
                DateTime dtSearchDayLimit = new DateTime(dt.Year, dt.Month, dt.Day, 23, 59, 59, 999);
                DateTime dtSearchDayStart = new DateTime(dt.Year, dt.Month, dt.Day);
                DateTime dtSearchDayFinish = new DateTime(dt.Year, dtSearch.Month, dtSearch.Day, 23, 59, 59, 999);

                foreach (KeyValuePair<DateTime, DailySchedule> kvp in list)
                {
                    DateTime dtSchedule = kvp.Key;
                    DailySchedule ds = kvp.Value;

                    if (dtSchedule >= dtSearchDayStart)
                    {
                        // We have hit a day in the future that potentially has a next event.
                        // It may not have a next event though, if the timestamp passed in has
                        // gone beyond the last event time on this day's schedule.

                        if (dtSchedule > dtSearchDayFinish)
                        {
                            dtSearch = dtSearchDayStart;
                        }

                        StateChange scNext = ds.GetNextStateChange(dtSearch);

                        if (scNext.State != StateChange.INVALID_STATE)
                        {
                            // Fabricate a new state change that includes the target date
                            return scNext.GetCommittedCopy(dtSchedule);
                        }
                        // else, search the next day
                    }
                }

                // If not yet returned, we have run off the end of the container, so return empty state change
                return new StateChange();
            }



            private StateChange getPreviousDatedSC(DateTime dt, SortedList<DateTime, DailySchedule> list)
            {
                DateTime dtSearch = dt; // DateTime is a value type - you get a copy
                DateTime dtSearchDayStart = new DateTime(dt.Year, dt.Month, dt.Day);
                DateTime dtSearchDayFinish = new DateTime(dt.Year, dtSearch.Month, dtSearch.Day, 23, 59, 59, 999);

                foreach (KeyValuePair<DateTime, DailySchedule> kvp in list.Reverse())
                {
                    DateTime dtSchedule = kvp.Key;
                    DailySchedule ds = kvp.Value;

                    if (dtSchedule <= dtSearchDayStart)
                    {
                        // If we have gone past the actual day, reset the search time component
                        // Note: This needs to be a date only comparison.
                        if (dtSchedule < dtSearchDayStart)
                        {
                            dtSearch = dtSearchDayFinish;
                        }

                        StateChange scPrev = ds.GetPreviousStateChange(dtSearch);

                        if (scPrev.State != StateChange.INVALID_STATE)
                        {
                            // Fabricate a new state change that includes the target date
                            return scPrev.GetCommittedCopy(dtSchedule);
                        }
                        // else, go to the next day
                    }
                }

                // If not yet returned, we have run off the start of the container, so return empty state change
                return new StateChange();
            }


            /// <summary>
            /// Apply the DSLookup to retrieve the sorted list of specific dated DailySchedule objects
            /// </summary>
            /// <returns>
            /// SortedList of type DateTime to DailySchedule;
            /// </returns>
            /// <remarks>
            /// This function effectively just converts the DailySchedule name key to an actual DailySchedule object
            /// in the returned SortedList.
            /// </remarks>
            private SortedList<DateTime, DailySchedule> getSpecificDS()
            {
                SortedList<DateTime, DailySchedule> listReturn = new SortedList<DateTime, DailySchedule>(new ByDTDateAscending());

                foreach (KeyValuePair<DateTime, string> kvp in _specificDateDSs)
                {
                    string sDSName = kvp.Value;
                    DailySchedule ds = lookupDSfromName(sDSName);
                    if (ds != null)
                    {
                        listReturn[kvp.Key] = ds;
                    }

                }

                return listReturn;
            }

            /// <summary>
            /// Retrieve a DailySchedule object from the lookup by it's name.
            /// </summary>
            /// <param name="sDSName">string; DailySchedule name</param>
            /// <returns>
            /// Object of type DailySchedule; NULL reference is the object does not exist
            /// </returns>
            private DailySchedule lookupDSfromName(string sDSName)
            {
                var prefix = "lookupDSfromName() - ";

                DailySchedule ds;
                if (!_dsLookup.TryGetValue(sDSName, out ds))
                {
                    // ERROR
                    string msg = string.Format("Failed to find DailySchedule object with name={0} in DSLookup.", sDSName);
                    logger.Error(prefix + msg);
                }

                return ds;
            }

            /// <summary>
            /// Try to retrieve year from cache, and create if it does not exist
            /// </summary>
            /// <returns>
            /// SortedList of type DateTime to DailySchedule;
            /// The DateTime index is only relevant for the Date part.
            /// </returns>
            private SortedList<DateTime, DailySchedule> getOrCreateYearlyDS(int year)
            {
                SortedList<DateTime, DailySchedule> listReturn;
                if (_cachedYearlyDSs.TryGetValue(year, out listReturn))
                {
                    // Exists for this year
                    return listReturn;
                }
                else
                {
                    // Does not exist, create, store, and return
                    listReturn = applyYear(year);

                    if (listReturn.Any())
                        _cachedYearlyDSs[year] = listReturn;

                    return listReturn;
                }
            }


            /// <summary>
            /// Make a copy of the RepeatingYearly StateChanges, and set the indices year to the argument
            /// </summary>
            /// <returns></returns>
            private SortedList<DateTime, DailySchedule> applyYear(int year)
            {
                SortedList<DateTime, DailySchedule> listReturn = new SortedList<DateTime, DailySchedule>(new ByDTDateAscending());

                foreach (KeyValuePair<DateTime, string> kvp in _repeatYearlyDSs)
                {
                    // Make a new dateTime for this year, from the template dateTime
                    DateTime dtYear = setYear(kvp.Key, year);

                    // Convert string name to actual DailySchedule object
                    string sDSName = kvp.Value;
                    DailySchedule ds = lookupDSfromName(sDSName);
                    if (ds != null)
                    {
                        listReturn.Add(dtYear, ds);
                    }
                }

                // Add runoff bookends, First for next year, Last for previous year
                if (_repeatYearlyDSs.Any())
                {
                    // First of next year
                    KeyValuePair<DateTime, string> kvpFirst = _repeatYearlyDSs.First();
                    DailySchedule dsFirst = lookupDSfromName(kvpFirst.Value);
                    DateTime dtYearNext = setYear(kvpFirst.Key, year + 1);
                    if (dsFirst != null)
                    {
                        listReturn.Add(dtYearNext, dsFirst);
                    }

                    // Last of previous year
                    KeyValuePair<DateTime, string> kvpLast = _repeatYearlyDSs.Last();
                    DailySchedule dsLast = lookupDSfromName(kvpLast.Value);
                    DateTime dtYearPrev = setYear(kvpFirst.Key, year - 1);
                    if (dsLast != null)
                    {
                        listReturn.Add(dtYearPrev, dsLast);
                    }

                }

                return listReturn;
            }


            /// <summary>
            /// Take a DateTime, and return a new DateTime with the requested year
            /// </summary>
            /// <param name="dtTemplate"></param>
            /// <returns></returns>
            private DateTime setYear(DateTime dtTemplate, int year)
            {
                return new DateTime(year, dtTemplate.Month, dtTemplate.Day, dtTemplate.Hour, dtTemplate.Minute, dtTemplate.Second, dtTemplate.Millisecond);
            }


            private SortedList<DateTime, DailySchedule> applyWeek(DateTime dt)
            {
                SortedList<DateTime, DailySchedule> listReturn = new SortedList<DateTime, DailySchedule>(new ByDTDateAscending());

                // Sanity
                if (_defaultWS.Count() == 0)
                    return listReturn;

                // Get the day of the week
                int iDayOfWeek = (int)dt.DayOfWeek;
                TimeSpan tsDayAdjustment = new TimeSpan(iDayOfWeek, 0, 0, 0);

                // Work out the starting dates for the weeks
                DateTime dtDateOnly = new DateTime(dt.Year, dt.Month, dt.Day);


                DateTime startDateThisWeek = dtDateOnly.Subtract(tsDayAdjustment);
                DateTime startDateNextWeek = startDateThisWeek.Add(new TimeSpan(7, 0, 0, 0));
                DateTime startDateLastWeek = startDateThisWeek.Subtract(new TimeSpan(7, 0, 0, 0));

                // Instantiate a container with a weekly schedule that encompasses the passed in
                // date, and a weekly schedule that is one week in advance.

                Dictionary<int, DailySchedule> wsDic = _defaultWS.GetWeeklyScheduleDictionary();
                foreach (KeyValuePair<int, DailySchedule> kvp in wsDic)
                {
                    int day = kvp.Key;
                    DailySchedule ds = kvp.Value;

                    DateTime dtKeyThisWeek = startDateThisWeek.Add(new TimeSpan(day, 0, 0, 0));
                    DateTime dtKeyNextWeek = startDateNextWeek.Add(new TimeSpan(day, 0, 0, 0));
                    DateTime dtKeyLastWeek = startDateLastWeek.Add(new TimeSpan(day, 0, 0, 0));

                    listReturn.Add(dtKeyThisWeek, ds);
                    listReturn.Add(dtKeyNextWeek, ds);
                    listReturn.Add(dtKeyLastWeek, ds);
                }

                return listReturn;
            }

            public override string ToString()
            {
                lock (_locker)
                {
                    string sRet = _name + "[" + _pkey+  "]";
                    if (!string.IsNullOrWhiteSpace(_target))
                    {
                        sRet += "@" + _target;
                    }
                    return sRet;
                }
            }

            public string ToDiagnosticString()
            {
                lock (_locker)
                {
                    string sReturn = _name + ": ";

                    if (!string.IsNullOrWhiteSpace(_target))
                        sReturn += TargetKey + ":" + _target + ",";
                    else
                        sReturn += "(No Target),";

                    sReturn += TimeZoneInfoKey + ":" + _tzi.Id;
                    sReturn += ",";

                    sReturn += DefaultWSKey + ":" + _defaultWS.ToString();
                    sReturn += ",";

                    int iOnce = 0;

                    // DSLookup
                    sReturn += DSLookupKey + ":[";
                    foreach (KeyValuePair<string, DailySchedule> kvpC in _dsLookup)
                    {
                        // kvp.Key = name of DailySchedule
                        // kvp.Value = DailySchedule

                        if (iOnce > 0)
                            sReturn += ",";

                        sReturn += "{";

                        sReturn += DailyScheduleNameKey + ":" + kvpC.Key;
                        sReturn += ",";
                        sReturn += DailyScheduleKey + ":{" + kvpC.Value.ToString() + "}";

                        sReturn += "}";

                        iOnce = 1;
                    }

                    sReturn += "]";

                    sReturn += ",";

                    iOnce = 0;
                    sReturn += RepeatYearliesKey + ":[";
                    foreach (KeyValuePair<DateTime, string> kvpA in _repeatYearlyDSs)
                    {
                        // kvp.Key == DateTime (Repeat Date)
                        // kvp.Value == name of DailySchedule

                        if (iOnce > 0)
                            sReturn += ",";

                        sReturn += "{";

                        sReturn += RepeatYearlyKey + ":" + kvpA.Key.ToString(StateChange.DateFormat);
                        sReturn += ",";
                        sReturn += RepeatYearlyDSKey + ":{" + kvpA.Value + "}";

                        sReturn += "}";

                        iOnce = 1;
                    }
                    sReturn += "]";

                    sReturn += ",";


                    sReturn += SpecificDatesKey + ":";

                    sReturn += "[";
                    iOnce = 0;
                    foreach (KeyValuePair<DateTime, string> kvpB in _specificDateDSs)
                    {
                        // kvp.Key == DateTime (Repeat Date)
                        // kvp.Value == name of DailySchedule

                        if (iOnce > 0)
                            sReturn += ",";

                        sReturn += "{";

                        sReturn += SpecificDateKey + ":" + kvpB.Key.ToString(StateChange.DateFormat);
                        sReturn += ",";
                        sReturn += SpecificDateDSKey + ":{" + kvpB.Value + "}";

                        sReturn += "}";

                        iOnce = 1;
                    }
                    sReturn += "]";

                    return sReturn;
                }
            }


            public static string ToJson(Schedule schd)
            {
                if (schd == null)
                    return "";

                lock (schd._locker)
                {
                    string sReturn = string.Empty;

                    sReturn += "{";

                    // Name
                    sReturn += "\"" + NameKey + "\":";
                    sReturn += "\"" + schd.Name + "\",";

                    // TimeZoneInfo
                    sReturn += "\"" + TimeZoneInfoKey + "\":";
                    sReturn += "\"" + schd.TimeZoneInfo.Id + "\",";

                    // Weekly Schedule
                    sReturn += "\"" + DefaultWSKey + "\":";
                    sReturn += WeeklySchedule.ToJson(schd._defaultWS);
                    sReturn += ",";


                    /////////////
                    // DSLookup

                    sReturn += "\"" + DSLookupKey + "\":";

                    int iOnce = 0;

                    sReturn += "[";

                    foreach (KeyValuePair<string, DailySchedule> kvpC in schd._dsLookup)
                    {
                        // kvp.Key == name of DailySchedule
                        // kvp.Value == DailySchedule

                        if (iOnce > 0)
                            sReturn += ",";

                        sReturn += "{";

                        sReturn += "\"" + DailyScheduleNameKey + "\":";
                        sReturn += "\"" + kvpC.Key + "\"";

                        sReturn += ",";

                        sReturn += "\"" + DailyScheduleKey + "\":";
                        sReturn += DailySchedule.ToJson(kvpC.Value);

                        sReturn += "}";
                        iOnce = 1;
                    }

                    sReturn += "],";


                    ///////////////////
                    // Specific Dates

                    sReturn += "\"" + SpecificDatesKey + "\":";

                    sReturn += "[";

                    iOnce = 0;
                    foreach (KeyValuePair<DateTime, string> kvpA in schd._specificDateDSs)
                    {
                        // kvp.Key = DateTime; Date that schedule applies to
                        // kvp.Value = DailySchedule object

                        if (iOnce > 0)
                            sReturn += ",";

                        sReturn += "{";

                        sReturn += "\"" + SpecificDateKey + "\":";
                        sReturn += "\"" + kvpA.Key.ToString(StateChange.DateFormat) + "\""; // DateTime, as string, quoted

                        sReturn += ",";

                        sReturn += "\"" + DailyScheduleNameKey + "\":";
                        sReturn += "\"" + kvpA.Value + "\"";

                        sReturn += "}";
                        iOnce = 1;
                    }

                    sReturn += "],";



                    ////////////////////////
                    // RepeatYearly Events

                    sReturn += "\"" + RepeatYearliesKey + "\":";

                    sReturn += "[";

                    iOnce = 0;
                    foreach (KeyValuePair<DateTime, string> kvpB in schd._repeatYearlyDSs)
                    {
                        // kvp.Key = DateTime; Date that schedule applies to
                        // kvp.Value = DailySchedule object

                        if (iOnce > 0)
                            sReturn += ",";

                        sReturn += "{";

                        sReturn += "\"" + RepeatYearlyKey + "\":";
                        sReturn += "\"" + kvpB.Key.ToString(StateChange.DateFormat) + "\""; // DateTime, as string, quoted
                        sReturn += ",";

                        sReturn += "\"" + DailyScheduleNameKey + "\":";
                        sReturn += "\"" + kvpB.Value + "\"";

                        sReturn += "}";

                        iOnce = 1;
                    }

                    sReturn += "]";


                    sReturn += "}";

                    return sReturn;
                }
            }

            public static Schedule FromJson(string source)
            {
                var prefix = "FromJson() - ";

                JavaScriptSerializer serializer = new JavaScriptSerializer();
                Dictionary<string, Object> dicSource = null;
                try
                {
                    dicSource = (Dictionary<string, Object>)serializer.Deserialize<object>(source);
                }
                catch (Exception ex)
                {
                    string error = string.Format("Failed to deserialize source string to Dictionary of string-to-object; Source: {0}; Error: {1}", source, ex.Message);
                    logger.Warn(prefix + error);
                    return null;
                }

                if (dicSource == null)
                    return null;
                else
                    return ImportFromDictionary(dicSource);
            }

            public static Schedule ImportFromDictionary(Dictionary<string, Object> dicSource)
            {
                Schedule schd = new Schedule();

                lock (schd._locker)
                {

                    ///////////////////////////////////////////////////////////////////
                    // Name

                    Object objName;
                    if (dicSource.TryGetValue(NameKey, out objName))
                    {
                        schd.Name = (string)objName;
                    }


                    ///////////////////////////////////////////////////////////////////
                    // TimeZoneInfo

                    Object objTimeZoneInfo;
                    if (dicSource.TryGetValue(TimeZoneInfoKey, out objTimeZoneInfo))
                    {
                        string sTimeZoneInfoId = (string)objTimeZoneInfo;
                        TimeZoneInfo tzi = getTimeZoneInfoFromString(sTimeZoneInfoId);
                        schd.TimeZoneInfo = tzi;
                    }


                    ///////////////////////////////////////////////////////////////////
                    // Default Weekly Schedule

                    Object objDefaultWS;
                    if (dicSource.TryGetValue(DefaultWSKey, out objDefaultWS))
                    {
                        // Expect a dictionary of string-to-Object
                        Dictionary<string, Object> dicDefaultWS = (Dictionary<string, Object>)objDefaultWS;

                        schd._defaultWS = WeeklySchedule.ImportFromDictionary(dicDefaultWS);
                    }


                    ///////////////////////////////////////////////////////////////////
                    // DSLookup

                    Object objArrayDSLookup;
                    if (dicSource.TryGetValue(DSLookupKey, out objArrayDSLookup))
                    {
                        // objArrayDSLookup is an array of Dictionaries that have two entries,
                        // the name of the schedule and a Json representation of the DailySchedule.

                        Object[] arrayDSLookup = (Object[])objArrayDSLookup;
                        foreach (Object objDSNameDSPair in arrayDSLookup)
                        {
                            Dictionary<string, Object> dicDSNameDSPair = (Dictionary<string, Object>)objDSNameDSPair;

                            // 1st object is the name of the DailySchedule
                            Object objDSName1;
                            string sDSName1 = string.Empty;
                            if (dicDSNameDSPair.TryGetValue(DailyScheduleNameKey, out objDSName1))
                            {
                                sDSName1 = (string)objDSName1;
                            }

                            // 2nd object is the JSON representation of the DailySchedule 
                            Object objDS;
                            DailySchedule ds = new DailySchedule();
                            if (dicDSNameDSPair.TryGetValue(DailyScheduleKey, out objDS))
                            {
                                // Each object is a dictionary representing a Json object.
                                // Each daily schedule has a name, and an array of StateChange objects.
                                Dictionary<string, Object> dicDS = (Dictionary<string, Object>)objDS;

                                ds = DailySchedule.ImportFromDictionary(dicDS);
                            }

                            schd._dsLookup[sDSName1] = ds;
                        }
                    }



                    ///////////////////////////////////////////////////////////////////
                    // RepeatYearlies

                    Object objRepeatYearlies;
                    if (dicSource.TryGetValue(RepeatYearliesKey, out objRepeatYearlies))
                    {
                        Object[] arrayRYs = (Object[])objRepeatYearlies;
                        foreach (Object objJsonRY in arrayRYs)
                        {
                            // Each object is a dictionary, representing a Json object.
                            // Each dictionary should have two entries, the date that the
                            // daily schedule applies to, and the name of daily schedule.
                            Dictionary<string, Object> dicRY = (Dictionary<string, Object>)objJsonRY;

                            // The two Json properties are the two keys, RepeatYearlyKey (DateTime) and 
                            // DailyScheduleNameKey for the name of the DailySchedule

                            Object objRepeatDateTimeString;
                            string sRepeatDateTime = string.Empty;
                            DateTime dtRepeat = DateTime.MinValue;

                            // 1st object is the Repeating Date
                            if (dicRY.TryGetValue(RepeatYearlyKey, out objRepeatDateTimeString))
                            {
                                sRepeatDateTime = (string)objRepeatDateTimeString;

                                // Expect "0001-12-25" eg "yyyy-MM-dd" (Note year will be meaningless and should be minimum value)
                                DateTime.TryParseExact(sRepeatDateTime, StateChange.DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dtRepeat);
                            }

                            // 2nd object is the name of the DailySchedule
                            Object objDSName2;
                            string sDSName2 = string.Empty;
                            if (dicRY.TryGetValue(DailyScheduleNameKey, out objDSName2))
                            {
                                sDSName2 = (string)objDSName2;
                            }

                            // If good, commit to storage
                            if (dtRepeat != DateTime.MinValue)
                            {
                                schd._repeatYearlyDSs.Add(dtRepeat, sDSName2);
                            }
                        }
                    }



                    ///////////////////////////////////////////////////////////////////
                    // Specific Dates

                    Object objSpecificDates;
                    if (dicSource.TryGetValue(SpecificDatesKey, out objSpecificDates))
                    {
                        // Expect an array of JSON objects, each of which has a DateTime and a DailySchedule
                        Object[] arraySDs = (Object[])objSpecificDates;
                        foreach (Object objJsonSD in arraySDs)
                        {
                            Dictionary<string, Object> dicSD = (Dictionary<string, Object>)objJsonSD;

                            Object objSpecificDateTimeString;
                            string sSpecificDateTime = string.Empty;
                            DateTime dtSpecific = DateTime.MinValue;

                            // 1st object is the specific date
                            if (dicSD.TryGetValue(SpecificDateKey, out objSpecificDateTimeString))
                            {
                                sSpecificDateTime = (string)objSpecificDateTimeString;

                                // Expect "2014-12-25" eg "yyyy-MM-dd"
                                DateTime.TryParseExact(sSpecificDateTime, StateChange.DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dtSpecific);
                            }

                            // 2nd object is the name of the DailySchedule
                            Object objDSName3;
                            string sDSName3 = string.Empty;
                            if (dicSD.TryGetValue(DailyScheduleNameKey, out objDSName3))
                            {
                                sDSName3 = (string)objDSName3;
                            }

                            if (dtSpecific != DateTime.MinValue)
                            {
                                schd._specificDateDSs.Add(dtSpecific, sDSName3);
                            }
                        }
                    }

                    return schd;
                }
            }



            /// <summary>
            /// Take a string, representing a TimeZoneInfo.Id, and return the TimeZoneInfo object
            /// </summary>
            /// <param name="sTimeZoneInfoId">string; TimeZoneInfo Id property</param>
            /// <returns>
            /// Object of type TimeZoneInfo; 
            /// Result of conversion attempt;
            /// Defaults to TimeZoneInfo.Utc in event of recoverable error;
            /// </returns>
            private static TimeZoneInfo getTimeZoneInfoFromString(string sTimeZoneInfoId)
            {
                TimeZoneInfo tziReturn = TimeZoneInfo.Utc;

                try
                {
                    tziReturn = TimeZoneInfo.FindSystemTimeZoneById(sTimeZoneInfoId);
                }
                catch (Exception ex)
                {
                    if (ex is ArgumentNullException ||
                        ex is TimeZoneNotFoundException ||
                        ex is InvalidTimeZoneException)
                    {
                        // For no timezone, or unintelligible timezone, return UTC
                        return TimeZoneInfo.Utc;
                    }

                    // else, pass upwards...
                    throw;
                }

                // catcher
                return tziReturn;
            }






            // To use the Schedule object as a Dictionary key, the Equals and GetHashCode
            // functions have to be overridden.  Usually these would use ToString() to 
            // stream the object and then compare the streams.  In this case, the name is
            // used to speed up the comparison and avoid the streaming step.

            public override bool Equals(object other)
            { return (other.ToString() == ToString()); }

            public override int GetHashCode()
            { return (ToString().GetHashCode()); }


        } // end of class Schedule









        /// <summary>
        /// Collection of state changes for one week
        /// </summary>
        public class WeeklySchedule
        {

            ///////////////////
            // STATIC MEMBERS

            private static log4net.ILog logger = log4net.LogManager.GetLogger("Scheduler_WeeklySchedule");

            private static Int64 _instanceCount;
            private static Object _lockerStatic;

            public static string NameKey;
            public static string DayOfWeekKey;
            public static string WeeklyScheduleKey;
            public static string DailyScheduleKey;
            public static string DailyScheduleNameKey;
            public static string DSLookupKey;

            public static string NameStem;



            /////////////////////
            // INSTANCE MEMBERS

            // DailySchedules container holds a set of strings (DailySchedule names) in a dictionary, where the 
            // key is the int value of the DayOfWeek system enumeration.
            // The string is used to lookup which schedule to use on that day.

            private string _name;
            private Dictionary<int, string> _dailySchedules = new Dictionary<int, string>();
            private Dictionary<string, DailySchedule> _dsLookup = new Dictionary<string, DailySchedule>();






            //////////
            // CTORS

            // Static
            static WeeklySchedule()
            {
                _lockerStatic = new Object();
                NameKey = "Name";
                DayOfWeekKey = "Day";
                DailyScheduleKey = "DS";
                DailyScheduleNameKey = "DSName";
                DSLookupKey = "Wk_DSLookup";
                WeeklyScheduleKey = "WkSch";
                NameStem = "WkSchd_";
            }

            // Default
            public WeeklySchedule()
            {
                _name = getNextName();
            }

            // Specific
            public WeeklySchedule(string name)
            {
                _name = name;
            }


            // Copy
            public WeeklySchedule(WeeklySchedule wsOther)
            {
                _name = wsOther._name;
                _dailySchedules = new Dictionary<int, string>(wsOther._dailySchedules);
                _dsLookup = new Dictionary<string, DailySchedule>(wsOther._dsLookup);
            }




            //////////////
            // ACCESSORS

            public string Name
            {
                get { return _name; }
                set { _name = value; }
            }



            /////////////////////
            // MEMBER FUNCTIONS



            /// <summary>
            /// Set a daily schedule for a day of the week.
            /// </summary>
            /// <param name="day">DayOfWeek Enum; Eg DayOfWeek.Monday</param>
            /// <param name="ds">Scheduler.DailySchedule Object;</param>
            /// <remarks>
            /// Square brackets indexer is used to over-write existing schedule without qualms.
            /// </remarks>
            public void Add(DayOfWeek day, DailySchedule ds)
            {
                _dsLookup[ds.Name] = ds;
                _dailySchedules[(int)day] = ds.Name;
            }

            /// <summary>
            /// Add the DailySchedule to the lookup of available DailySchedules to use.
            /// </summary>
            /// <param name="ds">Scheduler.DailySchedule Object;</param>
            /// <remarks>
            /// This adds a DS to the available set of DS's, but does not select the DS 
            /// for use on a particular day.
            /// </remarks>
            public void Add(DailySchedule ds)
            {
                _dsLookup[ds.Name] = ds;
            }


            /// <summary>
            /// Use a DailySchedule that is already in the Lookup dictionary on a particular day.
            /// </summary>
            /// <param name="day">DayOfWeek Enum; Eg DayOfWeek.Monday</param>
            /// <param name="sDailyScheduleName">string; Name of DailySchedule to use on this day.</param>
            /// <returns>
            /// bool; true if the DailySchedule exists in the lookup, and can be used for this day.
            /// </returns>
            /// 
            public bool Use(DayOfWeek day, string sDailyScheduleName)
            {
                DailySchedule ds;
                if (_dsLookup.TryGetValue(sDailyScheduleName, out ds))
                {
                    // This DS exists, we can use it for the day
                    _dailySchedules[(int)day] = sDailyScheduleName;
                    return true;
                }

                // Name string did not provide access to an existing DS, fail...
                return false;
            }

            /// <summary>
            /// Remove a daily schedule for a day of the week.
            /// </summary>
            /// <param name="day">DayOfWeek enum</param>
            /// <returns>bool; Flag to indicate successful removal (wrapper for Dictionary.Remove())</returns>
            public bool Remove(DayOfWeek day)
            {
                return _dailySchedules.Remove((int)day);
            }

            /// <summary>
            /// Return a count of the days that are set in the weekly schedule.
            /// </summary>
            /// <returns>int; Number of days (0-7) that are set in the weekly schedule.</returns>
            public int Count()
            {
                return _dailySchedules.Count();
            }

            /// <summary>
            /// Retrieve a Dictionary of int (DayOfWeek Enum) to DailySchedule
            /// </summary>
            /// <returns>
            /// Dictionary of type int to DailySchedule; 
            /// int key is converted value of DayOfWeek Enumeration.
            /// DailySchedule value is the DailySchedule to use for that day of the week.
            /// </returns>
            public Dictionary<int, DailySchedule> GetWeeklyScheduleDictionary()
            {
                var prefix = "GetWeeklyScheduleDictionary() - ";

                Dictionary<int, DailySchedule> dicReturn = new Dictionary<int, DailySchedule>();

                foreach (KeyValuePair<int, string> kvp in _dailySchedules)
                {
                    // kvp.Key == int, day of week enum
                    // kvp.Value == string, name of DailySchedule

                    string sDSName = kvp.Value;
                    DailySchedule ds;

                    if (_dsLookup.TryGetValue(sDSName, out ds))
                    {
                        dicReturn[kvp.Key] = ds;
                    }
                    else
                    {
                        string msg = string.Format("Failed to find a DailySchedule in the DSLookup with name={0}", sDSName);
                        logger.Error(prefix + msg);
                    }
                }

                return dicReturn;
            }

            /// <summary>
            /// Get a copy of the DS objects in the lookup
            /// </summary>
            /// <returns></returns>
            public Dictionary<string, DailySchedule> GetDailySchedulesCopy()
            {
                Dictionary<string, DailySchedule> DSs = new Dictionary<string, DailySchedule>();
                foreach (KeyValuePair<string, DailySchedule> kvp1 in _dsLookup)
                {
                    // kvp1.Key = DailySchedule name
                    // kvp1.Value = DailySchedule
                    DSs.Add(kvp1.Key, new DailySchedule(kvp1.Value));
                }

                return DSs;
            }


            public override string ToString()
            {
                string sReturn = _name + ":";


                // DailySchedule names selected for each day

                sReturn += WeeklyScheduleKey + ":[";
                int iOnce = 0;
                foreach (KeyValuePair<int, string> kvp1 in _dailySchedules)
                {
                    // kvp.Key = int; DayOfWeek enumeration
                    // kvp.Value = name of DailySchedule

                    if (iOnce > 0)
                        sReturn += ",";

                    sReturn += "{";

                    sReturn += ((DayOfWeek)kvp1.Key).ToString();
                    sReturn += ",";
                    sReturn += kvp1.Value;

                    sReturn += "}";

                    iOnce = 1;
                }

                sReturn += "],";


                // DSLookup
                iOnce = 0;
                sReturn += DSLookupKey + ":[";

                foreach (KeyValuePair<string, DailySchedule> kvp2 in _dsLookup)
                {
                    // kvp.Key = name of DailySchedule
                    // kvp.Value = Daily Schedule

                    if (iOnce > 0)
                        sReturn += ",";

                    sReturn += "{";

                    sReturn += kvp2.Key;
                    sReturn += ",";
                    sReturn += kvp2.Value.ToString();

                    sReturn += "}";

                    iOnce = 1;
                }

                sReturn += "]";

                return sReturn;
            }


            ////////////
            // HELPERS



            private static string getNextName()
            {
                lock (_lockerStatic)
                {
                    ++_instanceCount;
                    return NameStem + _instanceCount.ToString();
                }
            }


            public static string ToJson(WeeklySchedule ws)
            {
                if (ws == null)
                    return "";

                string sReturn = string.Empty;

                sReturn += "{";

                sReturn += "\"" + NameKey + "\":";
                sReturn += "\"" + ws._name + "\",";

                int iOnce = 0;

                sReturn += "\"" + DSLookupKey + "\":";
                sReturn += "[";

                foreach (KeyValuePair<string, DailySchedule> kvpA in ws._dsLookup)
                {
                    // kvp.Key = name of DailySchedule
                    // kvp.Value = DailySchedule

                    if (iOnce > 0)
                        sReturn += ",";

                    sReturn += "{";

                    sReturn += "\"" + DailyScheduleNameKey + "\":";
                    sReturn += "\"" + kvpA.Key + "\"";
                    sReturn += ",";

                    sReturn += "\"" + DailyScheduleKey + "\":";
                    sReturn += DailySchedule.ToJson(kvpA.Value);

                    sReturn += "}";

                    iOnce = 1;
                }

                sReturn += "],";

                iOnce = 0;

                sReturn += "\"" + WeeklyScheduleKey + "\":";
                sReturn += "[";

                foreach (KeyValuePair<int, string> kvpB in ws._dailySchedules)
                {
                    // kvp.Key = int; DayOfWeek enumeration value as int
                    // kvp.Value = name of DailySchedule

                    if (iOnce > 0)
                        sReturn += ",";

                    sReturn += "{";

                    sReturn += "\"" + DayOfWeekKey + "\":";
                    sReturn += kvpB.Key.ToString(); // int, as string.
                    sReturn += ",";

                    sReturn += "\"" + DailyScheduleNameKey + "\":";
                    sReturn += "\"" + kvpB.Value + "\"";

                    sReturn += "}";

                    iOnce = 1;
                }

                sReturn += "]";

                sReturn += "}";

                return sReturn;
            }


            public static WeeklySchedule FromJson(string source)
            {
                var prefix = "FromJson() - ";

                JavaScriptSerializer serializer = new JavaScriptSerializer();
                Dictionary<string, Object> dicSource = null;
                try
                {
                    dicSource = (Dictionary<string, Object>)serializer.Deserialize<object>(source);
                }
                catch (Exception ex)
                {
                    string error = string.Format("Failed to deserialize source string to Dictionary of string-to-object; Source: {0}; Error: {1}", source, ex.Message);
                    logger.Warn(prefix + error);
                    return null;
                }

                if (dicSource == null)
                    return null;
                else
                    return ImportFromDictionary(dicSource);
            }


            public static WeeklySchedule ImportFromDictionary(Dictionary<string, Object> dicSource)
            {
                var prefix = "ImportFromDictionary() - ";

                WeeklySchedule ws = new WeeklySchedule();

                // Name
                Object objWSName;
                if (dicSource.TryGetValue(NameKey, out objWSName))
                {
                    ws.Name = (string)objWSName;
                }

                // DSLookup
                Object objArrayDSLookup;
                if (dicSource.TryGetValue(DSLookupKey, out objArrayDSLookup))
                {
                    // objArrayDSLookup is an array of Dictionaries that have two entries,
                    // the name of the schedule and a Json representation of the DailySchedule.

                    Object[] arrayDSLookup = (Object[])objArrayDSLookup;
                    foreach (Object objDSNameDSPair in arrayDSLookup)
                    {
                        Dictionary<string, Object> dicDSNameDSPair = (Dictionary<string, Object>)objDSNameDSPair;

                        // 1st object is the name of the DailySchedule
                        Object objDSName1;
                        string sDSName1 = string.Empty;
                        if (dicDSNameDSPair.TryGetValue(DailyScheduleNameKey, out objDSName1))
                        {
                            sDSName1 = (string)objDSName1;
                        }

                        // 2nd object is the JSON representation of the DailySchedule 
                        Object objDS;
                        DailySchedule ds = new DailySchedule();
                        if (dicDSNameDSPair.TryGetValue(DailyScheduleKey, out objDS))
                        {
                            // Each object is a dictionary representing a Json object.
                            // Each daily schedule has a name, and an array of StateChange objects.
                            Dictionary<string, Object> dicDS = (Dictionary<string, Object>)objDS;

                            ds = DailySchedule.ImportFromDictionary(dicDS);
                        }

                        // Add the DailySchedule to the lookup dictionary
                        ws.Add(ds);
                    }
                }


                // WeeklySchedule
                Object objArrayDSNames;
                if (dicSource.TryGetValue(WeeklyScheduleKey, out objArrayDSNames))
                {
                    // objArrayDSNamess is an array of Dictionaries that have two entries,
                    // the day that the daily schedule applies to, and the name of the DailySchedule

                    Object[] arrayDSNames = (Object[])objArrayDSNames;
                    foreach (Object objDayDSNamePair in arrayDSNames)
                    {
                        // Each object is a dictionary, representing a Json object.
                        // Each dictionary should have two entries, the day that the
                        // daily schedule applies to, and the daily schedule itself.
                        Dictionary<string, Object> dicDayDSNamePair = (Dictionary<string, Object>)objDayDSNamePair;

                        // 1st object is the day of the week enum coded as an integer
                        Object objDayOfWeek;
                        int iDayOfWeek = -1;
                        if (dicDayDSNamePair.TryGetValue(DayOfWeekKey, out objDayOfWeek))
                        {
                            iDayOfWeek = (int)objDayOfWeek;
                        }

                        // 2nd object is the name of a DailySchedule
                        Object objDSName2;
                        string sDSName2 = string.Empty;
                        if (dicDayDSNamePair.TryGetValue(DailyScheduleNameKey, out objDSName2))
                        {
                            // Each object is a string
                            sDSName2 = (string)objDSName2;
                        }

                        if (!ws.Use((DayOfWeek)iDayOfWeek, sDSName2))
                        {
                            // ERROR
                            string msg = string.Format("Failed to find a DailySchedule in the DSLookup with name={0} for WeeklySchedule={1}", sDSName2, ws.Name);
                            logger.Error(prefix + msg);
                        }
                    }
                }

                return ws;
            }


        } // end of WeeklySchedule class








        /// <summary>
        /// A collection of state changes for one day.
        /// </summary>
        public class DailySchedule
        {

            ///////////////////
            // STATIC MEMBERS

            private static log4net.ILog logger = log4net.LogManager.GetLogger("Scheduler_DailySchedule");

            private static Int64 _instanceCount;
            private static Object _lockerStatic;

            public static string NameKey;
            public static string StateChangesKey;
            public static string NameStem;


            /////////////////////
            // INSTANCE MEMBERS

            private string _name;
            private SortedList<DateTime, StateChange> _stateChanges = new SortedList<DateTime, StateChange>(new ByDTTimeAscending());

            // Note: Sorting by Time descending so that when we search the set for the next state
            //       change, we search in chronologically reversed order.  This means that when
            //       the search time is passed, we have already seen the next event, so all we have
            //       to do is keep track of the previous event on each iteration.



            //////////
            // CTORS

            // Static
            static DailySchedule()
            {
                _lockerStatic = new Object();
                _instanceCount = 0;

                NameKey = "Name";
                StateChangesKey = "SCs";
                NameStem = "DailySchedule_";
            }

            // Default
            public DailySchedule()
            {
                _name = getNextName();
            }

            // Specific
            public DailySchedule(string name)
            {
                _name = name;
            }

            // Copy
            public DailySchedule(DailySchedule rhsDS)
            {
                _name = rhsDS._name;
                _stateChanges = new SortedList<DateTime, StateChange>(rhsDS._stateChanges);
            }





            //////////////
            // ACCESSORS

            public string Name
            {
                get { return _name; }
                set { _name = value; }
            }



            /////////////////////
            // MEMBER FUNCTIONS


            public SortedList<DateTime, StateChange> GetStateChangesCopy()
            {
                SortedList<DateTime, StateChange> SCs = new SortedList<DateTime, StateChange>();
                foreach (KeyValuePair<DateTime, StateChange> kvp1 in _stateChanges)
                {
                    // kvp1.Key = DateTime
                    // kvp1.Value = StateChange
                    SCs.Add(kvp1.Key, new StateChange(kvp1.Value));
                }

                return SCs;
            }

            public bool Add(StateChange sc)
            {
                // If we have this change, or it's not valid, return false
                if (_stateChanges.ContainsKey(sc.Time) || sc.State == StateChange.INVALID_STATE)
                    return false;

                _stateChanges.Add(sc.Time, sc);
                return true;
            }

            public bool Add(DateTime dt, int state)
            {
                return Add(new StateChange(dt, state));
            }

            public bool Remove(StateChange sc)
            {
                return _stateChanges.Remove(sc.Time);
            }

            public int Count()
            {
                return _stateChanges.Count();
            }


            public override string ToString()
            {
                string sReturn = _name + ":";

                sReturn += "[";

                int iOnce = 0;
                foreach (KeyValuePair<DateTime, StateChange> kvp in _stateChanges)
                {
                    // kvp.Key = DateTime
                    // kvp.Value = StateChange

                    if (iOnce > 0)
                        sReturn += ",";

                    sReturn += kvp.Value.ToString();

                    iOnce = 1;
                }

                sReturn += "]";

                return sReturn;
            }

            /// <summary>
            /// Output to JSON format
            /// </summary>
            /// <param name="ds">Object of type DailySchedule</param>
            /// <returns>string; Object in JSON format</returns>
            public static string ToJson(DailySchedule ds)
            {
                if (ds == null)
                    return "";

                string sReturn = string.Empty;

                sReturn += "{";

                sReturn += "\"" + NameKey + "\":";
                sReturn += "\"" + ds._name + "\",";

                sReturn += "\"" + StateChangesKey + "\":";

                sReturn += "[";

                int iOnce = 0;
                foreach (KeyValuePair<DateTime, StateChange> kvp in ds._stateChanges)
                {
                    // kvp.Key = DateTime
                    // kvp.Value = StateChange

                    if (iOnce > 0)
                        sReturn += ",";

                    sReturn += StateChange.ToJson(kvp.Value);

                    iOnce = 1;
                }

                sReturn += "]";

                sReturn += "}";

                return sReturn;
            }



            public static DailySchedule FromJson(string source)
            {
                var prefix = "FromJson() - ";

                JavaScriptSerializer serializer = new JavaScriptSerializer();
                Dictionary<string, Object> dicSource = null;
                try
                {
                    dicSource = (Dictionary<string, Object>)serializer.Deserialize<object>(source);
                }
                catch (Exception ex)
                {
                    string error = string.Format("Failed to deserialize source string to Dictionary of string-to-object; Source: {0}; Error: {1}", source, ex.Message);
                    logger.Warn(prefix + error);
                    return null;
                }

                if (dicSource == null)
                    return null;
                else
                    return ImportFromDictionary(dicSource);
            }


            public static DailySchedule ImportFromDictionary(Dictionary<string, Object> dicSource)
            {
                DailySchedule ds = new DailySchedule();

                // Name
                Object objName;
                if (dicSource.TryGetValue(NameKey, out objName))
                {
                    ds.Name = (string)objName;
                }

                // StateChanges
                Object objArraySCs;
                if (dicSource.TryGetValue(StateChangesKey, out objArraySCs))
                {
                    // objArraySCs in an array of Objects.
                    // These objects are Dictionary<string,Object>, which is how reloaded
                    // Json objects are represented, with each property being the key, and
                    // each value for the property being an object representation of the value.

                    Object[] array = (Object[])objArraySCs;
                    foreach (Object objJsonSC in array)
                    {
                        Dictionary<string, Object> dicStateChange = (Dictionary<string, Object>)objJsonSC;

                        StateChange sc = StateChange.ImportFromDictionary(dicStateChange);

                        if (sc.State != StateChange.INVALID_STATE)
                            ds.Add(sc);
                    }
                }

                return ds;
            }


            /// <summary>
            /// Get the next state change after the given time
            /// </summary>
            /// <param name="dt">DateTime; DateTime in UTC, where only the time part is considered;</param>
            /// <returns>Object of type StateChange; The next state change after the passed in time.</returns>
            /// <remarks>
            /// If there are no StateChange elements yet created, the fn will return an empty StateChange
            /// object that has a state of UNKNOWN and a time of 00:00:00.000
            /// </remarks>
            public StateChange GetNextStateChange(DateTime dt)
            {
                TimeSpan timeOfDay = dt.TimeOfDay;
                DateTime dt0 = DateTime.MinValue.Add(timeOfDay);

                StateChange scNext = new StateChange();

                foreach (KeyValuePair<DateTime, StateChange> kvp in _stateChanges.Reverse())
                {
                    // kvp.Key = DateTime
                    // kvp.Value = StateChange

                    if (kvp.Value.Time <= dt0)
                    {
                        break;
                    }

                    scNext = kvp.Value;
                }

                return scNext;
            }

            /// <summary>
            /// Get the previous state change before or at the given time
            /// </summary>
            /// <param name="dt">DateTime; DateTime in UTC, where only the time part is considered;</param>
            /// <returns>Object of type StateChange; The next state change after the passed in time.</returns>
            /// <remarks>
            /// If there are no StateChange elements yet created, the fn will return an empty StateChange
            /// object that has a state of UNKNOWN and a time of 00:00:00.000
            /// </remarks>
            public StateChange GetPreviousStateChange(DateTime dt)
            {
                TimeSpan timeOfDay = dt.TimeOfDay;
                DateTime dt0 = DateTime.MinValue.Add(timeOfDay);

                StateChange scPrev = new StateChange();

                foreach (KeyValuePair<DateTime, StateChange> kvp in _stateChanges)
                {
                    // kvp.Key = DateTime
                    // kvp.Value = StateChange

                    if (kvp.Value.Time > dt0)
                    {
                        break;
                    }

                    scPrev = kvp.Value;
                }

                return scPrev;
            }


            ////////////
            // HELPERS 

            private static string getNextName()
            {
                lock (_lockerStatic)
                {
                    ++_instanceCount;
                    return NameStem + _instanceCount.ToString();
                }
            }


        } // end of class DailySchedule








        public class StateChange
        {
            ///////////////////
            // STATIC MEMBERS

            private static log4net.ILog logger = log4net.LogManager.GetLogger("Scheduler_StateChange");

            private static DateTime TimeMax;
            public static string StateKey;
            public static string DateTimeKey;
            public static string ScheduleTargetKey;
            public static string TimeFormatWithMillis;
            public static string TimeFormatNoMillis;
            public static string DateFormat;
            public static string UniversalDBFormat;
            public static readonly int INVALID_STATE;


            /////////////////////
            // INSTANCE MEMBERS

            private DateTime _dt;
            private int _state;
            private string _scheduleTarget;



            //////////
            // CTORS

            // Static 
            static StateChange()
            {
                TimeMax = DateTime.MinValue.Add(new TimeSpan(1, 0, 0, 0));
                StateKey = "S";
                DateTimeKey = "T";
                ScheduleTargetKey = "O"; // for Owner
                TimeFormatWithMillis = "HH:mm:ss.fff";
                TimeFormatNoMillis = "HH:mm:ss";
                DateFormat = "yyyy-MM-dd";
                UniversalDBFormat = DateFormat + "T" + TimeFormatWithMillis;
                INVALID_STATE = -1;
            }


            // Default 
            public StateChange()
            {
                _dt = DateTime.MinValue;
                _state = INVALID_STATE;
                _scheduleTarget = string.Empty;
            }


            // Specific
            public StateChange(DateTime dt, int state, String scheduleTarget = "")
            {
                _dt = dt;
                setState(state);

                if (!string.IsNullOrWhiteSpace(scheduleTarget))
                {
                    _scheduleTarget = scheduleTarget;
                }
            }


            /// <summary>
            /// Ctor; Build an object from string and integer, primarily for Json/Database rehydration
            /// </summary>
            /// <param name="sDateAndOrTime">string; "yyyy-MM-ddTHH:mm:ss.fff" or "yyyy-MM-dd" or "HH:mm:ss.fff"</param>
            /// <param name="iState">int; State value; State to change to at this time</param>
            public StateChange(string sDateAndOrTime, int iState, String scheduleTarget = "")
            {
                setDateTime(sDateAndOrTime);
                setState(iState);

                if (!string.IsNullOrWhiteSpace(scheduleTarget))
                {
                    _scheduleTarget = scheduleTarget;
                }
            }


            // Copy
            public StateChange(StateChange rhsSC)
            {
                _dt = rhsSC._dt;
                _state = rhsSC._state;
                _scheduleTarget = rhsSC._scheduleTarget;
            }


            //////////////
            // ACCESSORS

            public int State
            {
                get { return _state; }
                set { setState(value); }
            }

            public string ScheduleTarget
            {
                get { return _scheduleTarget; }
                set { _scheduleTarget = value; }
            }

            public DateTime DateAndTime
            {
                get { return _dt; }
                set { _dt = value; }
            }

            public DateTime Date
            {
                get { return _dt.Date; }
                set
                {
                    TimeSpan time = _dt.TimeOfDay;
                    _dt = (value.Date).Add(time);
                }
            }

            public DateTime Time
            {
                get { return (DateTime.MinValue).Add(_dt.TimeOfDay); }
                set
                {
                    TimeSpan time = value.TimeOfDay;
                    _dt = (_dt.Date).Add(time);
                }
            }




            ////////////
            // HELPERS


            private void setState(int state)
            {
                if (state != INVALID_STATE)
                    _state = state;
            }

            private void setTime(string sTime)
            {
                // Expect "02:30:00.000" or "02:30:00"
                DateTime dt = DateTime.MinValue;
                if (DateTime.TryParseExact(sTime, TimeFormatWithMillis, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt))
                    this.Time = dt;
                else if (DateTime.TryParseExact(sTime, TimeFormatNoMillis, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt))
                    this.Time = dt;
            }

            private void setDate(string sDate)
            {
                // Expect "2014-12-31"
                DateTime dt = DateTime.MinValue;
                if (DateTime.TryParseExact(sDate, DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt))
                    this.Date = dt;
            }

            private void setDateTime(string sDateTime)
            {
                // Attempted semi-intelligence
                bool hasT = sDateTime.IndexOfAny("T".ToCharArray()) != -1;
                bool hasHyphen = sDateTime.IndexOfAny("-".ToCharArray()) != -1;
                bool hasColon = sDateTime.IndexOfAny(":".ToCharArray()) != -1;

                if (hasT)
                {
                    // Probably universal database format
                    DateTime dt = DateTime.MinValue;
                    if (DateTime.TryParseExact(sDateTime, UniversalDBFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt))
                        this.DateAndTime = dt;
                }
                else if (hasHyphen)
                {
                    // Probably date with no time
                    setDate(sDateTime);
                }
                else if (hasColon)
                {
                    // Probably time with no date
                    setTime(sDateTime);
                }
            }

            public override string ToString()
            {
                string sReturn = string.Empty;


                if (!string.IsNullOrWhiteSpace(_scheduleTarget))
                {
                    sReturn += _scheduleTarget + "|";
                }

                sReturn += _state.ToString() + "@";

                if (_dt.Date == DateTime.MinValue.Date)
                    sReturn += _dt.ToString(TimeFormatWithMillis);
                else
                    sReturn += _dt.ToString(UniversalDBFormat);

                return sReturn;
            }

            public override bool Equals(object obj)
            { return (obj.ToString() == this.ToString()); }

            public static bool operator ==(StateChange a, StateChange b)
            {
                // If both are null, or both are same instance, return true.
                if (System.Object.ReferenceEquals(a, b))
                {
                    return true;
                }

                // If one is null, but not both, return false.
                if (((object)a == null) || ((object)b == null))
                {
                    return false;
                }

                // Return true if the member variables match:
                return ((a._dt == b._dt) && (a._state == b._state) && (a._scheduleTarget == b._scheduleTarget));
            }

            public static bool operator !=(StateChange a, StateChange b)
            { return !(a == b); }

            public override int GetHashCode()
            { return (this.ToString().GetHashCode()); }


            /// <summary>
            /// Return a state change object where the date part has been adjusted to the arguments date.
            /// </summary>
            /// <param name="dtDateOnly"></param>
            /// <returns>
            /// Object of type StateChange; 
            /// Copy of the original object (state,time) but with the Date part changed to the arguments date.
            /// </returns>
            /// <remarks>
            /// The date argument may have a time component, it will be ignored.  
            /// Only the date is transferred.
            /// </remarks>
            public StateChange GetCommittedCopy(DateTime dtDateOnly)
            {
                StateChange scReturn = new StateChange(this);
                scReturn.Date = dtDateOnly;
                return scReturn;
            }


            public static bool IsValid(StateChange sc)
            {
                if (sc == null) return false;

                return (sc.State != INVALID_STATE);
            }

            /// <summary>
            /// Take a StateChange and return it's JSON representation as a string.
            /// </summary>
            /// <param name="sc">Object of type StateChange;</param>
            /// <returns>string; JSON representation of object</returns>
            public static string ToJson(StateChange sc)
            {
                if (sc == null)
                    return "";

                string sReturn = string.Empty;

                sReturn += "{";

                if (!string.IsNullOrWhiteSpace(sc._scheduleTarget))
                {
                    sReturn +=
                    "\"" +
                    ScheduleTargetKey +
                    "\":\"" +
                    sc._scheduleTarget +
                    "\",";
                }

                sReturn +=
                    "\"" +
                    StateKey +
                    "\":" +
                    sc.State.ToString() +
                    ",\"" +
                    DateTimeKey +
                    "\":\"";

                if (sc._dt.Date == DateTime.MinValue.Date)
                {
                    // Time only
                    if (sc._dt.Millisecond > 0)
                    {
                        sReturn += sc._dt.ToString(TimeFormatWithMillis);
                    }
                    else
                    {
                        sReturn += sc._dt.ToString(TimeFormatNoMillis);
                    }
                }
                else
                {
                    // DateTime
                    sReturn += sc._dt.ToString(UniversalDBFormat);
                }

                sReturn += "\"}";

                //MSJsonSerializer effort: {"S":2,"T":"\/Date(-62135587800000)\/"}
                //Custom: 
                //
                // {"S":2,"T":"02:30:00.000"}
                // or 
                // {"S":2,"T":"2014-12-31T02:30:00.000"}
                //
                // or, with a target schedule...
                // {"O":"MyFirstSchedule","S":2,"T":"2014-12-31T02:30:00.000"}

                return sReturn;
            }

            /// <summary>
            /// Take a JSON string representation of a StateChange, and return a StateChange object.
            /// </summary>
            /// <param name="source">string; JSON representation of object</param>
            /// <returns>Object of type StateChange;</returns>
            public static StateChange FromJson(string source)
            {
                var prefix = "FromJson() - ";

                JavaScriptSerializer serializer = new JavaScriptSerializer();
                Dictionary<string, Object> dicSource = null;
                try
                {
                    dicSource = (Dictionary<string, Object>)serializer.Deserialize<object>(source);
                }
                catch (Exception ex)
                {
                    string error = string.Format("Failed to deserialize source string to Dictionary of string-to-object; Source: {0}; Error: {1}", source, ex.Message);
                    logger.Warn(prefix + error);
                    return null;
                }

                if (dicSource == null)
                    return null;
                else
                    return ImportFromDictionary(dicSource);
            }

            public static StateChange ImportFromDictionary(Dictionary<string, Object> dicSC)
            {
                StateChange sc = new StateChange();

                int iState = 0;
                string sDateTime = string.Empty;
                string sSchedule = string.Empty;

                Object objSchedule;
                if (dicSC.TryGetValue(ScheduleTargetKey, out objSchedule))
                {
                    sSchedule = (string)objSchedule;
                }

                Object objState;
                if (dicSC.TryGetValue(StateKey, out objState))
                {
                    iState = (int)objState;
                }

                Object objDateTimeString;
                if (dicSC.TryGetValue(DateTimeKey, out objDateTimeString))
                {
                    sDateTime = (string)objDateTimeString;
                }

                if (iState != 0 && !string.IsNullOrWhiteSpace(sDateTime))
                {
                    sc = new StateChange(sDateTime, iState);
                    if (!string.IsNullOrWhiteSpace(sSchedule))
                    {
                        sc.ScheduleTarget = sSchedule;
                    }
                }

                return sc;
            }

        } // end of Scheduler.StateChange






        ///////////////////////////////////////////////////////////////////////////
        // COMPARATORS
        //

        //////////////////////
        // Tuple2Key sorters

        public class ByTuple2KeyDateTimeScheduleAscending : IComparer<Tuple2Key<DateTime, Schedule>>
        {
            public int Compare(Tuple2Key<DateTime, Schedule> x, Tuple2Key<DateTime, Schedule> y)
            {
                // First is DateTime
                if (x.First > y.First)
                    return 1;
                else if (x.First < y.First)
                    return -1;
                else
                {
                    // DateTime components match, x.First == y.First
                    // Differentiate by String component.
                    // Note: Ordinal sort is effectively sorting by Unicode value

                    return String.Compare(x.Second.Target, y.Second.Target, StringComparison.OrdinalIgnoreCase);
                }
            }
        }

        public class ByTuple2KeyDateTimeScheduleDescending : IComparer<Tuple2Key<DateTime, Schedule>>
        {
            public int Compare(Tuple2Key<DateTime, Schedule> x, Tuple2Key<DateTime, Schedule> y)
            {
                // First is DateTime
                if (x.First < y.First)
                    return 1;
                else if (x.First > y.First)
                    return -1;
                else
                {
                    // DateTime components match, x.First == y.First
                    // Differentiate by String component.
                    // Note: Ordinal sort is effectively sorting by Unicode value

                    return String.Compare(y.Second.Target, x.Second.Target, StringComparison.OrdinalIgnoreCase);
                }
            }
        }


        //////////////////////////
        // Date and Time sorters

        public class ByDTDateAndTimeAscending : IComparer<DateTime>
        {
            public int Compare(DateTime x, DateTime y)
            {
                if (x > y)
                    return 1;
                else if (x < y)
                    return -1;
                else
                    return 0;
            }
        }

        public class ByDTDateAndTimeDescending : IComparer<DateTime>
        {
            public int Compare(DateTime x, DateTime y)
            {
                if (x < y)
                    return 1;
                else if (x > y)
                    return -1;
                else
                    return 0;
            }
        }


        // End of Date and Time sorters
        /////////////////////////////////



        /////////////////
        // Date sorters

        public class ByDTDateAscending : IComparer<DateTime>
        {
            public int Compare(DateTime x, DateTime y)
            {
                if (x.Date > y.Date)
                    return 1;
                else if (x.Date < y.Date)
                    return -1;
                else
                    return 0;
            }
        }

        public class ByDTDateDescending : IComparer<DateTime>
        {
            public int Compare(DateTime x, DateTime y)
            {
                if (x.Date < y.Date)
                    return 1;
                else if (x.Date > y.Date)
                    return -1;
                else
                    return 0;
            }
        }

        // End of Date sorters
        ////////////////////////




        /////////////////
        // Time sorters

        public class ByDTTimeAscending : IComparer<DateTime>
        {
            public int Compare(DateTime x, DateTime y)
            {
                if (x.TimeOfDay > y.TimeOfDay)
                    return 1;
                else if (x.TimeOfDay < y.TimeOfDay)
                    return -1;
                else
                    return 0;
            }
        }

        public class ByDTTimeDescending : IComparer<DateTime>
        {
            public int Compare(DateTime x, DateTime y)
            {
                if (x.TimeOfDay < y.TimeOfDay)
                    return 1;
                else if (x.TimeOfDay > y.TimeOfDay)
                    return -1;
                else
                    return 0;
            }
        }

        public class BySCTimeAscending : IComparer<StateChange>
        {
            public int Compare(StateChange x, StateChange y)
            {
                if (x.Time > y.Time)
                    return 1;
                else if (x.Time < y.Time)
                    return -1;
                else
                    return x.GetHashCode().CompareTo(y.GetHashCode());
            }
        }

        public class BySCTimeDescending : IComparer<StateChange>
        {
            public int Compare(StateChange x, StateChange y)
            {
                if (x.Time < y.Time)
                    return 1;
                else if (x.Time > y.Time)
                    return -1;
                else
                    return x.GetHashCode().CompareTo(y.GetHashCode());
            }
        }

        // End of Time sorters
        ////////////////////////


    } // end of class Scheduler

} // end of namespace NZ01






///////////////////////////////////////////////////////////////////////////////
// GARBAGE
//

/*
/// <summary>
/// Remove a schedule from the queue and registry to de-activate it.
/// </summary>
/// <param name="pkey">Int64; Schedule pkey</param>
/// <param name="symbol">string; Symbol; Schedule target market</param>
/// <returns>bool; true if found and successfully de-registered, else false</returns>
public bool Unregister(Int64 pkey, string symbol)
{
    var prefix = string.Format("Unregister(pkey={0}, symbol={1}) - ", pkey, symbol);
    
    // Sanity
    if (pkey <= 0)
    {
        logger.Warn(prefix + "An invalid pkey value was passed as an argument; Exiting, no op");
        return false;
    }

    if (string.IsNullOrWhiteSpace(symbol))
    {
        logger.Warn(prefix + "No symbol string was passed as an argument; Exiting, no op");
        return false;
    }

    bool result = true;
                
    lock(_locker)
    {
        List<Schedule> schedules = getRegisteredScheduleInstances(pkey, symbol);

        if (schedules.Count() == 0)
        {
            string msgScheduleNotFound = "Request to delete schedule instance failed because schedule was not found in registry.";
            logger.Warn(prefix + msgScheduleNotFound);
            return false;
        }
        else if (schedules.Count() > 1)
        {
            string msgTooManySchedules = "There should be only one instance of a schedule for a market; {0} instances were found; Will remove all instances.";
            logger.Warn(prefix + msgTooManySchedules);
            // continue and remove all
        }

        foreach (Schedule schedule in schedules)
        {
            bool resultThisAttempt = drop(schedule);
            if (resultThisAttempt)
            {
                string msgDropGood = "Attempt to drop schedule instance succeeded.";
                logger.Info(prefix + msgDropGood);
            }
            else
            {
                string msgDropBad = "Failed to drop schedule instance.";
                logger.Warn(prefix + msgDropBad);
            }

            result = result && resultThisAttempt;
        }
    }

    return result;
}
*/


/*
private List<Schedule> getRegisteredScheduleInstances(Int64 pkey, string symbol = "")
{
    List<Schedule> schedules = new List<Schedule>();

    // Iterate registry and build list of refs to schedule instances that have this pkey
    foreach(KeyValuePair<Schedule,StateChange> kvp in _registry)
    {
        Schedule schd = kvp.Key;
        if (schd.Pkey == pkey)
        {
            if (string.IsNullOrWhiteSpace(symbol))
            {
                // No Symbol is specified - match any symbol
                schedules.Add(schd);
            }
            else
            {
                // Symbol is specified - match on symbol
                if (schd.Target == symbol)
                {
                    schedules.Add(schd);
                }
            }
        }
    }

    return schedules;
}
*/

//private List<Schedule> getRegisteredScheduleInstancesByPkey(Int64 pkey)
//{
//    return _pkeyToTargetedSchedules.Get(pkey);
//}



/*
public static Dictionary<String, Int64> GetSchedulesOwned(string username)
{
    Dictionary<String, Int64> schedulesOwned = new Dictionary<String, Int64>();

    // Lock the object
    // Copy the catalog
    // Iterate and find schedules owned by this user

    return schedulesOwned;
}
*/














