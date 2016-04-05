using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Configuration;
using System.Data.Common;

using Quartz.Impl.Matchers;
using Quartz.Spi;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

using Quartz.Collection;
using Quartz.Simpl;

using Raven.Client.Linq;

namespace Quartz.Impl.RavenDB
{
    public class RavenJobStore : IJobStore
    {
        private readonly object lockObject = new object();
        private TimeSpan misfireThreshold = TimeSpan.FromSeconds(5);
        private ISchedulerSignaler signaler;
        private static long ftrCtr = SystemTime.UtcNow().Ticks;
        private readonly TreeSet<Trigger> timeTriggers = new TreeSet<Trigger>(new TriggerComparator());

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;
        public bool Clustered => false;

        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        public static string defaultConnectionString = "Url=http://localhost:8080;DefaultDatabase=IftahDB";
        public static string Url { get; set; }
        public static string DefaultDatabase { get; set; }

        public RavenJobStore()
        {
            var stringBuilder = new DbConnectionStringBuilder();

            if (ConfigurationManager.ConnectionStrings["quartznet-ravendb"] != null)
                stringBuilder.ConnectionString = ConfigurationManager.ConnectionStrings["quartznet-ravendb"].ConnectionString;
            else
                stringBuilder.ConnectionString = defaultConnectionString;
 
            Url = stringBuilder["Url"] as string;
            DefaultDatabase = stringBuilder["DefaultDatabase"] as string;

            InstanceName = "UnitTestScheduler";
            InstanceId = "instance_two";
            
            // This must be replaced with RecoverJobs() for persistance...
            // let's clean up just to make the scheduler work first
            ClearAllSchedulingData();
        }
        
        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler s)
        {
            signaler = s;
            StoreScheduler(true);
        }

        public void SetSchedulerState(string state)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                sched.State = state;
                session.Store(sched);
                session.SaveChanges();
            }
        }

        public void SchedulerStarted()
        {
            
            try
            {
                //RecoverJobs();
            }
            catch (SchedulerException se)
            {
                throw new SchedulerConfigException("Failure occurred during job recovery.", se);
            }
            
            SetSchedulerState("Started");
        }

        public void SchedulerPaused()
        {
            SetSchedulerState("Paused");
        }

        public void SchedulerResumed()
        {
            SetSchedulerState("Resumed");
        }

        public void Shutdown()
        {
            SetSchedulerState("Shutdown");
        }
        
        /// <summary>
        /// Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        protected virtual string GetFiredTriggerRecordId()
        {
            var value = Interlocked.Increment(ref ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            StoreJob(newJob, true);
            StoreTrigger(newTrigger, true);
        }

        public bool IsJobGroupPaused(string groupName)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                return sched.PausedJobGroups.Contains(groupName);
            }
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                return sched.PausedTriggerGroups.Contains(groupName);
            }
        }

        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            if (CheckExists(newJob.Key))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }
            }
            
            var job = new Job(newJob);

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                // Store() overwrites if job id already exists
                session.Store(job, job.Key.Name + "/" + job.Key.Group);
                session.SaveChanges();
            }
        }

        public void StoreScheduler(bool replaceExisting)
        {
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(InstanceName);
            if (docMetaData != null)
            {
                if (!replaceExisting)
                {
                    // Scheduler with same instance name already exists, no need to initialize it
                    return;
                }
            }

            // Create new empty scheduler and store it
            var schedToStore = new Scheduler
            {
                InstanceName = this.InstanceName,
                LastCheckinTime = DateTimeOffset.MinValue,
                CheckinInterval = DateTimeOffset.MinValue,
                Calendars = new Dictionary<string, ICalendar>(),
                Locks = new Collection.HashSet<string>(),
                PausedJobGroups = new Collection.HashSet<string>(),
                PausedTriggerGroups = new Collection.HashSet<string>(),
                BlockedJobs = new Collection.HashSet<SimpleKey>()
            };

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                // Store() overites if id already exists
                session.Store(schedToStore, this.InstanceName);
                session.SaveChanges();
            }
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs, bool replace)
        {
            using (var bulkInsert = DocumentStoreHolder.Store.BulkInsert(options: new BulkInsertOptions() /*{ OverwriteExisting = replace }*/))
            {
                foreach (var pair in triggersAndJobs)
                {
                    bulkInsert.Store(new Job(pair.Key), pair.Key.Key.Name + "/" + pair.Key.Key.Group); // Store job first
                    foreach (var trig in pair.Value)
                    {
                        var operTrig = trig as IOperableTrigger;
                        if (operTrig == null) throw new InvalidCastException();

                        var trigger = new Trigger(operTrig);

                        if (GetPausedTriggerGroups().Contains(operTrig.Key.Group) || GetPausedJobGroups().Contains(operTrig.JobKey.Group))
                        {
                            trigger.State = InternalTriggerState.Paused;
                            if (GetBlockedJobs().Contains(new SimpleKey(operTrig.JobKey.Name, operTrig.JobKey.Group)))
                            {
                                trigger.State = InternalTriggerState.PausedAndBlocked;
                            }
                        }
                        else if (GetBlockedJobs().Contains(new SimpleKey(operTrig.JobKey.Name, operTrig.JobKey.Group)))
                        {
                            trigger.State = InternalTriggerState.Blocked;
                        }
                        else
                        {
                            timeTriggers.Add(trigger);
                        }

                        bulkInsert.Store(trigger, trigger.TriggerKey.Name + "/" + trigger.TriggerKey.Group); // Storing all triggers for a current job
                    }
                    
                }
                // bulkInsert is disposed - same effect as session.SaveChanges()
            }
        }

        public bool RemoveJob(JobKey jobKey)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                if (CheckExists(jobKey)) return false;

                session.Advanced.Defer(new DeleteCommandData
                {
                    Key = jobKey.Name + "/" + jobKey.Group
                });
                session.SaveChanges();
            }
            return true;    
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            // Returns false in case at least one job removal fails
            var result = true;
            foreach (var key in jobKeys)
            {
                result &= RemoveJob(key);
            }
            return result;
        }

        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var job = session.Load<Job>(jobKey.Name + "/" + jobKey.Group);

                return (job == null) ? null : job.Deserialize();
            }
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            if (CheckExists(newTrigger.Key))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }
            }

            if (!CheckExists(newTrigger.JobKey))
            {
                throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
            }

            var trigger = new Trigger(newTrigger);

            if (GetPausedTriggerGroups().Contains(newTrigger.Key.Group) || GetPausedJobGroups().Contains(newTrigger.JobKey.Group))
            {
                trigger.State = InternalTriggerState.Paused;
                if (GetBlockedJobs().Contains(new SimpleKey(newTrigger.JobKey.Name, newTrigger.JobKey.Group)))
                {
                    trigger.State = InternalTriggerState.PausedAndBlocked;
                }
            }
            else if (GetBlockedJobs().Contains(new SimpleKey(newTrigger.JobKey.Name, newTrigger.JobKey.Group)))
            {
                trigger.State = InternalTriggerState.Blocked;
            }
            else
            {
                timeTriggers.Add(trigger);
            }

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                // Overwrite if exists
                session.Store(trigger, trigger.TriggerKey.Name + "/" + trigger.TriggerKey.Group);
                session.SaveChanges();
            }
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey)) return false;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
                var job = RetrieveJob(new JobKey(trigger.JobKey.Name, trigger.JobKey.Group));
                var trigList = GetTriggersForJob(job.Key);
                timeTriggers.Remove(trigger);
                // Remove the trigger's job if it is not associated with any other triggers
                if ((trigList == null || trigList.Count == 0) && !job.Durable)
                {
                    if (RemoveJob(job.Key))
                    {
                        signaler.NotifySchedulerListenersJobDeleted(job.Key);
                    }
                }
                
            }
            return true;
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            // Returns false in case at least one trigger removal fails
            var result = true;
            foreach (var key in triggerKeys)
            {
                result &= RemoveTrigger(key);
            }
            return result;
        }

        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            if (!CheckExists(triggerKey)) return false;
            var wasRemoved = RemoveTrigger(triggerKey);
            if (wasRemoved)
            {
                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    var trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
                    timeTriggers.Remove(trigger);
                }
                StoreTrigger(newTrigger, true);
            }
            return wasRemoved;
        }

        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            // this check might not be necessary 
            if (!CheckExists(triggerKey)) return null;

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trig = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                return (trig == null) ? null : trig.Deserialize();
            }
        }

        public bool CalendarExists(string calName)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                return sched.Calendars.ContainsKey(calName);
            }
        }

        public bool CheckExists(JobKey jobKey)
        {   
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(jobKey.Name + "/" + jobKey.Group);
            return docMetaData != null;
            
            //can't use this way of checking because triggers and groups might be named the same so their type must be specified
            /*
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var job = session.Load<Job>(jobKey.Name + "/" + jobKey.Group);
                return job != null;
            }*/
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(triggerKey.Name + "/" + triggerKey.Group);
            return docMetaData != null;
            /*
            //can't use this way of checking because triggers and groups might be named the same so their type must be specified
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
                return trigger != null;
            }*/
        }

        public void ClearAllSchedulingData()
        {
            ClearAllSchedulingData<Scheduler>();
            ClearAllSchedulingData<Job>();
            ClearAllSchedulingData<Trigger>();
        }

        public void ClearAllSchedulingData<T>()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                List<T> documents;
                do
                {
                    // TODO: find a better way to handle more than 128 documents
                    documents = session.Query<T>().ToList();
                    foreach (var doc in documents)
                    {
                        session.Delete(doc);
                    }

                    session.SaveChanges();
                } while (documents.Count() == 128);
            }
        }

        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            var calendarCopy = (ICalendar)calendar.Clone();

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);

                if (sched?.Calendars == null) throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));


                if ((sched.Calendars.ContainsKey(name)) && (!replaceExisting)) throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture, "Calendar with name '{0}' already exists.", name));

                // add or replace calendar
                sched.Calendars[name] = calendarCopy;
                session.Store(sched, InstanceName);

                if (!updateTriggers)
                {
                    return;
                }

                var triggersToUpdate = session
                    .Query<Trigger>()
                    .Where(t => t.CalendarName == name)
                    .ToList();



                if (triggersToUpdate.Count == 0)
                {
                    session.SaveChanges();
                    return;
                }

                //using (var bulkInsert = DocumentStoreHolder.Store.BulkInsert(options: new BulkInsertOptions() /*{ OverwriteExisting = true }*/))
                //using (var session = DocumentStoreHolder.Store.OpenSession())
                //{
                foreach (var t in triggersToUpdate)
                {
                    var trigger = t.Deserialize();
                    bool removed = timeTriggers.Remove(t);
                    trigger.UpdateWithNewCalendar(calendarCopy, misfireThreshold);

                    var updatedTrigger = new Trigger(trigger)
                    {
                        State = t.State,
                    };
                    //overwrite
                    session.Store(updatedTrigger, trigger.Key.Name + "/" + trigger.Key.Group);
                    if (removed)
                    {
                        timeTriggers.Add(updatedTrigger);
                    }
                }
                session.SaveChanges();
                //}
            }
        }
        
        public bool RemoveCalendar(string calName)
        {
            if (RetrieveCalendar(calName) == null)
                return false;
            var calCollection = RetrieveCalendarCollection();
            calCollection.Remove(calName);

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                sched.Calendars = calCollection;
                session.Store(sched, InstanceName);
            }
            return true;
        }

        public ICalendar RetrieveCalendar(string calName)
        {
            var callCollection = RetrieveCalendarCollection();
            return callCollection.ContainsKey(calName) ? callCollection[calName] : null;
        }

        public Dictionary<string, ICalendar> RetrieveCalendarCollection()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                if (sched == null) throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                if (sched.Calendars == null) throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Calendar collection in '{0}' is null", InstanceName));
                return sched.Calendars;
            }
        }

        public int GetNumberOfJobs()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<Job>().Count();
            }
        }

        public int GetNumberOfTriggers()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<Trigger>().Count();
            }
        }

        public int GetNumberOfCalendars()
        {
            return RetrieveCalendarCollection().Count;
        }

        public Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;
            Collection.ISet<JobKey> result = new Collection.HashSet<JobKey>();

            if (op.Equals(StringOperator.Equality))
            {
                System.Collections.Generic.HashSet<SimpleKey> queryResult;
                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    queryResult = session
                        .Query<Job>()
                        .Where(j => j.Key.Group == compareToValue)
                        //.Select(j => new JobKey(j.Key.Name, j.Key.Group))
                        .Select(j => j.Key)
                        .ToHashSet();
                }
                foreach (var simpleKey in queryResult)
                {
                    result.Add(new JobKey(simpleKey.Name, simpleKey.Group));
                }
                return result;
                
            }
            else
            {
                throw new NotImplementedException();
            }

        }

        public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return (Collection.ISet<TriggerKey>)session
                    .Query<Trigger>()
                    .Where(t => op.Evaluate(t.TriggerKey.Group, compareToValue))
                    .Select(t => t.TriggerKey)
                    .ToHashSet();
            }
        }

        public IList<string> GetJobGroupNames()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<Job>()
                    .Select(j => j.Key.Group)
                    .Distinct()
                    .ToList();
            }
        }

        public IList<string> GetTriggerGroupNames()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<Trigger>()
                    .Select(t => t.TriggerKey.Group)
                    .Distinct()
                    .ToList();
            }
        }

        public IList<string> GetCalendarNames()
        {
            return RetrieveCalendarCollection().Keys.ToList();
        }

        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var simpleJobKey = new SimpleKey(jobKey.Name, jobKey.Group);
                return session
                    .Query<Trigger>()
                    .Where(t => Equals(t.JobKey, simpleJobKey))
                    .ToList()
                    .Select(trigger => trigger.Deserialize()).ToList();
            }
        }

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            Trigger trigger;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
            }

            if (trigger == null)
            {
                return TriggerState.None;
            }

            switch (trigger.State)
            {
                case InternalTriggerState.Complete:
                    return TriggerState.Complete;
                case InternalTriggerState.Paused:
                    return TriggerState.Paused;
                case InternalTriggerState.PausedAndBlocked:
                    return TriggerState.Paused;
                case InternalTriggerState.Blocked:
                    return TriggerState.Blocked;
                case InternalTriggerState.Error:
                    return TriggerState.Error;
                default:
                    return TriggerState.Normal;
            }
        }

        public void PauseTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey)) return;

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trig = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                // if the trigger doesn't exist or is "complete" pausing it does not make sense...
                if (trig == null) return;
                if (trig.State == InternalTriggerState.Complete) return;
                
                trig.State = (trig.State == InternalTriggerState.Blocked ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Paused);
                timeTriggers.Remove(trig);
                session.Store(trig);
                session.SaveChanges();
            }
        }

        public Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            if (op.Equals(StringOperator.Equality))
            {
                Collection.ISet<string> pausedGroupsSet;
                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    pausedGroupsSet = (Collection.ISet<string>) session
                        .Query<Trigger>()
                        .Where(t => t.TriggerKey.Group == compareToValue)
                        .Select(t => t.TriggerKey.Group)
                        .ToHashSet();
                }

                foreach (string pausedGroup in pausedGroupsSet)
                {
                    Collection.ISet<TriggerKey> keys = GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(pausedGroup));

                    foreach (TriggerKey key in keys)
                    {
                        PauseTrigger(key);
                    }
                }

                return pausedGroupsSet;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void PauseJob(JobKey jobKey)
        {
            IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                PauseTrigger(trigger.Key);
            }
        }

        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;
            List<string> pausedGroups = new List<String>();

            if (op.Equals(StringOperator.Equality))
            {

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    var sched = session.Load<Scheduler>(InstanceName);

                    if (sched.PausedJobGroups.Add(matcher.CompareToValue))
                    {
                        pausedGroups.Add(matcher.CompareToValue);
                    }
                    session.Store(sched);
                    session.SaveChanges();
                }
            }
            else
            {
                throw new NotImplementedException();
            }

            foreach (string groupName in pausedGroups)
            {
                foreach (JobKey jobKey in GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)))
                {
                    IList<IOperableTrigger> triggers = GetTriggersForJob(jobKey);
                    foreach (IOperableTrigger trigger in triggers)
                    {
                        PauseTrigger(trigger.Key);
                    }
                }
            }

            return pausedGroups;
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey)) return;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                // if the trigger is not paused resuming it does not make sense...
                if (trigger.State != InternalTriggerState.Paused &&
                    trigger.State != InternalTriggerState.PausedAndBlocked)
                {
                    return;
                }

                if (GetBlockedJobs().Contains(new SimpleKey(trigger.JobKey.Name, trigger.JobKey.Group)))
                {
                    trigger.State = InternalTriggerState.Blocked;
                }
                else
                {
                    trigger.State = InternalTriggerState.Waiting;
                }

                ApplyMisfire(trigger);

                if (trigger.State == InternalTriggerState.Waiting)
                {
                    timeTriggers.Add(trigger);
                }

                session.Store(trigger);
                session.SaveChanges();
            } 
        }

        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            Collection.ISet<string> groups = new Collection.HashSet<string>();

            Collection.ISet<TriggerKey> keys = GetTriggerKeys(matcher);

            foreach (TriggerKey triggerKey in keys)
            {
                groups.Add(triggerKey.Group);
                var trigger = RetrieveTrigger(triggerKey);
     
                if (trigger != null)
                {
                    var jobGroup = trigger.JobKey.Group;
                    if (GetPausedJobGroups().Contains(jobGroup))
                    {
                        continue;
                    }
                }
                ResumeTrigger(triggerKey);
            }
            foreach (var group in groups)
            {
                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    var sched = session.Load<Scheduler>(InstanceName);
                    sched.PausedTriggerGroups.Remove(group);
                    session.Store(sched);
                    session.SaveChanges();
                }
            }
            

            return new List<string>(groups);
        }

        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<Scheduler>(InstanceName).PausedTriggerGroups;
            }
        }

        public Collection.ISet<string> GetPausedJobGroups()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<Scheduler>(InstanceName).PausedJobGroups;
            }
        }

        public Collection.ISet<SimpleKey> GetBlockedJobs()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<Scheduler>(InstanceName).BlockedJobs;
            }
        }

        public void ResumeJob(JobKey jobKey)
        {
            IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                ResumeTrigger(trigger.Key);
            }
        }

        public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            Collection.ISet<string> resumedGroups = new Collection.HashSet<string>();

            Collection.ISet<JobKey> keys = GetJobKeys(matcher);

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);

                foreach (var pausedJobGroup in sched.PausedJobGroups)
                {
                    if (matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue))
                    {
                        resumedGroups.Add(pausedJobGroup);
                    }
                }

                foreach (var resumedGroup in resumedGroups)
                {
                    sched.PausedJobGroups.Remove(resumedGroup);
                }
                session.Store(sched);
                session.SaveChanges();
            }

            foreach (JobKey key in keys)
            {
                IList<IOperableTrigger> triggers = GetTriggersForJob(key);
                foreach (IOperableTrigger trigger in triggers)
                {
                    ResumeTrigger(trigger.Key);
                }
            }

            return resumedGroups;
        }

        public void PauseAll()
        {
            IList<string> triggerGroupNames = GetTriggerGroupNames();

            foreach (var groupName in triggerGroupNames)
            {
                PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
            }
        }

        public void ResumeAll()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);

                sched.PausedJobGroups.Clear();

                var triggerGroupNames = GetTriggerGroupNames();

                foreach (var groupName in triggerGroupNames)
                {
                    ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
        }

        /// <summary>
        /// Applies the misfire.
        /// </summary>
        /// <param name="trigger"></param>
        /// <returns></returns>
      /*  protected virtual bool ApplyMisfire(Trigger Trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = Trigger.NextFireTimeUtc;
            if (!tnft.HasValue || tnft.Value > misfireTime
                || Trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (Trigger.CalendarName != null)
            {
                cal = RetrieveCalendar(Trigger.CalendarName);
            }

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = Trigger.Deserialize();

            signaler.NotifyTriggerListenersMisfired(trig);
            trig.UpdateAfterMisfire(cal);
            StoreTrigger(trig, true);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                signaler.NotifySchedulerListenersFinalized(trig);

                Trigger.State = InternalTriggerState.Complete;
                Trigger.NextFireTimeUtc = trig.GetNextFireTimeUtc();
                Trigger.PreviousFireTimeUtc = trig.GetPreviousFireTimeUtc();

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    //Trigger.IsTimedTrigger = false;
                    session.Store(Trigger);
                    session.SaveChanges();
                }
            }
            else if (tnft.Equals(trig.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }
        */
        /// <summary>
        /// Applies the misfire.
        /// </summary>
        /// <param name="tw">The trigger wrapper.</param>
        /// <returns></returns>
        protected virtual bool ApplyMisfire(Trigger trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = trigger.NextFireTimeUtc;
            if (!tnft.HasValue || tnft.Value > misfireTime
                || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = RetrieveCalendar(trigger.CalendarName);
            }

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = trigger.Deserialize();
            signaler.NotifyTriggerListenersMisfired(trig);
            trig.UpdateAfterMisfire(cal);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                signaler.NotifySchedulerListenersFinalized(trig);

                // Prepare database trigger with new state and calculated fire times
                trigger.State = InternalTriggerState.Complete;
                trigger.NextFireTimeUtc = trig.GetNextFireTimeUtc();
                trigger.PreviousFireTimeUtc = trig.GetPreviousFireTimeUtc();

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    session.Store(trigger);
                    session.SaveChanges();
                }
                
                timeTriggers.Remove(trigger);
            }
            else if (tnft.Equals(trigger.NextFireTimeUtc))
            {
                return false;
            }

            return true;
        }

        public virtual IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            List<IOperableTrigger> result = new List<IOperableTrigger>();
            Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
            Collection.ISet<Trigger> excludedTriggers = new Collection.HashSet<Trigger>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            // return empty list if store has no triggers.
            if (timeTriggers.Count == 0)
            {
                return result;
            }
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                while (true)
                {
                    var trigger = timeTriggers.First();
                    if (trigger == null)
                    {
                        break;
                    }

                    if (!timeTriggers.Remove(trigger))
                    {
                        break;
                    }

                    if (trigger.NextFireTimeUtc == null)
                    {
                        continue;
                    }

                    if (ApplyMisfire(trigger))
                    {
                        if (trigger.NextFireTimeUtc != null)
                        {
                            timeTriggers.Add(trigger);
                        }
                        continue;
                    }

                    if (trigger.NextFireTimeUtc > noLaterThan + timeWindow)
                    {
                        timeTriggers.Add(trigger);
                        break;
                    }

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // put it back into the timeTriggers set and continue to search for next trigger.
                    JobKey jobKey = new JobKey(trigger.JobKey.Name, trigger.JobKey.Group);
                    Job job = session
                        .Load<Job>(trigger.JobKey.Name + "/" + trigger.JobKey.Group);

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            excludedTriggers.Add(trigger);
                            continue; // go to next trigger in store.
                        }
                        else
                        {
                            acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                        }
                    }

                    trigger.State = InternalTriggerState.Acquired;
                    trigger.FireInstanceId = GetFiredTriggerRecordId();

                    session.Store(trigger, trigger.TriggerKey.Name + "/" + trigger.TriggerKey.Group);

                    result.Add(trigger.Deserialize());

                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = trigger.NextFireTimeUtc;
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }
                session.SaveChanges();

            }

            // If we did excluded triggers to prevent ACQUIRE state due to DisallowConcurrentExecution, we need to add them back to store.
            if (excludedTriggers.Count > 0)
            {
                timeTriggers.AddAll(excludedTriggers);
            }
            return result;
        }
        
        public void ReleaseAcquiredTrigger(IOperableTrigger trig)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var  trigger = session.Load<Trigger>(trig.Key.Name + "/" + trig.Key.Group);
                if (trigger == null) //|| (Trigger.State != InternalTriggerState.Acquired))
                {
                    return;
                }
                trigger.State = InternalTriggerState.Waiting;
                timeTriggers.Add(trigger);
                session.Store(trigger);
                session.SaveChanges();
            }
        }
        

        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            List<TriggerFiredResult> results = new List<TriggerFiredResult>();
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                foreach (IOperableTrigger tr in triggers)
                {
                    // was the trigger deleted since being acquired?
                    var trigger = session.Load<Trigger>(tr.Key.Name + "/" + tr.Key.Group);

                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    if (trigger?.State != InternalTriggerState.Acquired)
                    {
                        continue;
                    }

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = RetrieveCalendar(trigger.CalendarName);
                        if (cal == null)
                        {
                            continue;
                        }
                    }
                    DateTimeOffset? prevFireTime = trigger.PreviousFireTimeUtc;
                    // in case trigger was replaced between acquiring and firing
                    timeTriggers.Remove(trigger);

                    var trig = trigger.Deserialize();
                    trig.Triggered(cal);

                    TriggerFiredBundle bndle = new TriggerFiredBundle(RetrieveJob(trig.JobKey),
                        trig,
                        cal,
                        false, SystemTime.UtcNow(),
                        trig.GetPreviousFireTimeUtc(), prevFireTime,
                        trig.GetNextFireTimeUtc());

                    IJobDetail job = bndle.JobDetail;

                    trigger.UpdateFireTimes(trig);
                    trigger.State = InternalTriggerState.Waiting;

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        List<Trigger> trigs = session.Query<Trigger>()
                            .Where(t => Equals(t.JobKey.Group, job.Key.Group)&& Equals(t.JobKey.Name, job.Key.Name))
                            .ToList();

                        foreach (Trigger t in trigs)
                        {
                            if (t.State == InternalTriggerState.Waiting)
                            {
                                t.State = InternalTriggerState.Blocked;
                            }
                            if (t.State == InternalTriggerState.Paused)
                            {
                                t.State = InternalTriggerState.PausedAndBlocked;
                            }
                            timeTriggers.Remove(t);
                        }
                        var sched = session.Load<Scheduler>(InstanceId);
                        sched.BlockedJobs.Add(new SimpleKey(job.Key.Name, job.Key.Group));
                        session.Store(sched);
                    }
                    else if (trigger.NextFireTimeUtc != null)
                    {
                        timeTriggers.Add(trigger);
                    }

                    results.Add(new TriggerFiredResult(bndle));
                }
                session.SaveChanges();
            }
            return results;
        }

        public void TriggeredJobComplete(IOperableTrigger trig, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(trig.Key.Name + "/" + trig.Key.Group);
                var sched = session.Load<Scheduler>(InstanceName);

                // It's possible that the job or trigger is null if it was deleted during execution
                var job = session.Load<Job>(trig.JobKey.Name + "/" + trig.JobKey.Group);
                
                if (job != null)
                {
                    IJobDetail jd = job.Deserialize();

                    if (jobDetail.PersistJobDataAfterExecution)
                    {
                        JobDataMap newData = jobDetail.JobDataMap;
                        if (newData != null)
                        {
                            newData = (JobDataMap) newData.Clone();
                            newData.ClearDirtyFlag();
                        }
                        jd = jd.GetJobBuilder().SetJobData(newData).Build();
                        job = new Job(jd);
                        session.Store(job);
                    }
                    if (jd.ConcurrentExecutionDisallowed)
                    {
                        sched.BlockedJobs.Remove(new SimpleKey(jd.Key.Name, jd.Key.Group));

                        List<Trigger> trigs = session.Query<Trigger>()
                            .Where(t => Equals(t.JobKey.Group, jd.Key.Group) && Equals(t.JobKey.Name, jd.Key.Name))
                            .ToList();

                        foreach (Trigger t in trigs)
                        {
                            if (t.State == InternalTriggerState.Blocked)
                            {
                                t.State = InternalTriggerState.Waiting;
                                timeTriggers.Add(t);
                            }
                            if (t.State == InternalTriggerState.PausedAndBlocked)
                            {
                                t.State = InternalTriggerState.Paused;
                            }
                            session.Store(t);
                        }

                        signaler.SignalSchedulingChange(null);
                    }
                }
                else
                {
                    // even if it was deleted, there may be cleanup to do
                    sched.BlockedJobs.Remove(new SimpleKey(jobDetail.Key.Name, jobDetail.Key.Group));
                }

                // check for trigger deleted during execution...
                if (trigger != null)
                {
                    if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                    {
                        // Deleting triggers
                        DateTimeOffset? d = trig.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = trigger.NextFireTimeUtc;
                            if (!d.HasValue)
                            {
                                RemoveTrigger(trig.Key);
                            }
                            else
                            {
                                //Deleting cancelled - trigger still active
                            }
                        }
                        else
                        {
                            RemoveTrigger(trig.Key);
                            signaler.SignalSchedulingChange(null);
                        }
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                    {
                        trigger.State = InternalTriggerState.Complete;
                        timeTriggers.Remove(trigger);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                    {
                        //Log.Info(string.Format(CultureInfo.InvariantCulture, "Trigger {0} set to ERROR State.", trigger.Key));
                        trigger.State = InternalTriggerState.Error;
                        session.Store(trigger);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                    {
                        //Log.Info(string.Format(CultureInfo.InvariantCulture, "All triggers of Job {0} set to ERROR State.", trigger.JobKey));
                        SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Error);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                    {
                        SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Complete);
                        signaler.SignalSchedulingChange(null);
                    }
                }
                session.Store(sched);
                session.SaveChanges();
            }
        }

        /// <summary>
        /// Sets the State of all triggers of job to specified State.
        /// </summary>
        protected virtual void SetAllTriggersOfJobToState(JobKey jobKey, InternalTriggerState state)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                List<Trigger> trigs = session.Query<Trigger>()
                    .Where(t =>Equals(t.JobKey.Group, jobKey.Group) && Equals(t.JobKey.Name, jobKey.Name))
                    .ToList();

                foreach (var trig in trigs)
                {
                    trig.State = state;
                    if (state != InternalTriggerState.Waiting)
                    {
                        timeTriggers.Remove(trig);
                    }
                    session.Store(trig);
                }
                session.SaveChanges();
            }
        }

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            get { return misfireThreshold; }
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireThreshold must be larger than 0");
                }
                misfireThreshold = value;
            }
        }
    }
}