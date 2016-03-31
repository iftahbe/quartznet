using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Globalization;
using System.Linq;

using Quartz.Impl.Matchers;
using Quartz.Spi;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using System.Threading;

using Quartz.Simpl;

using Raven.Client.Linq;

namespace Quartz.Impl.RavenDB
{
    public class JobStore : IJobStore
    {
        private readonly object lockObject = new object();
        private TimeSpan misfireThreshold = TimeSpan.FromMinutes(1);
        private ISchedulerSignaler signaler;
        private static long ftrCtr = SystemTime.UtcNow().Ticks;

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;
        public bool Clustered => false;

        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        public JobStore()
        {
            InstanceName = "UnitTestScheduler";
            InstanceId = "instance_two";
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
                var sched = session.Load<RavenScheduler>(InstanceName);
                sched.State = state;
                session.Store(sched);
                session.SaveChanges();
            }
        }

        public void SchedulerStarted()
        {
            SetSchedulerState("Started");
            //TODO load or create new scheduler. 
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
                var sched = session.Load<RavenScheduler>(InstanceName);
                return sched.PausedJobGroups.Contains(groupName);
            }
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<RavenScheduler>(InstanceName);
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
            
            var job = new RavenJob(newJob);

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
                    throw new ObjectAlreadyExistsException(InstanceName);
                }
            }

            // Create new empty scheduler and store it
            var schedToStore = new RavenScheduler
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
                    bulkInsert.Store(new RavenJob(pair.Key), pair.Key.Key.Name + "/" + pair.Key.Key.Group); // Store job first
                    foreach (var trig in pair.Value) // Iterate over a set of triggers
                    {
                        var ravenTrigger = new RavenTrigger(trig as IOperableTrigger)
                        {
                            State = DetermineNewTriggerState(trig as IOperableTrigger)
                        };
                        
                        bulkInsert.Store(ravenTrigger, ravenTrigger.TriggerKey.Name + "/" + ravenTrigger.TriggerKey.Group); // Storing all triggers for a current job
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
                var job = session.Load<RavenJob>(jobKey.Name + "/" + jobKey.Group);

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

            if (CheckExists(newTrigger.JobKey))
            {
                if (!replaceExisting)
                {
                    throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
                }
            }

            var trigger = new RavenTrigger(newTrigger)
            {
                State = DetermineNewTriggerState(newTrigger),
                IsTimedTrigger = true
            };

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                // Overwrite if exists
                session.Store(trigger, trigger.TriggerKey.Name + "/" + trigger.TriggerKey.Group);
                session.SaveChanges();
            }
        }

        public InternalTriggerState DetermineNewTriggerState(IOperableTrigger newTrigger)
        {
            InternalTriggerState state;

            if (GetPausedTriggerGroups().Contains(newTrigger.Key.Group) || GetPausedJobGroups().Contains(newTrigger.JobKey.Group))
            {
                state = InternalTriggerState.Paused;
                if (GetBlockedJobs().Contains(new SimpleKey(newTrigger.JobKey.Name, newTrigger.JobKey.Group)))
                {
                    state = InternalTriggerState.PausedAndBlocked;
                }
            }
            else if (GetBlockedJobs().Contains(new SimpleKey(newTrigger.JobKey.Name, newTrigger.JobKey.Group)))
            {
                state = InternalTriggerState.Blocked;
            }
            else
            {
                //default is waiting
                state = InternalTriggerState.Waiting;
            }
            return state;
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey)) return false;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<RavenTrigger>(triggerKey.Name + "/" + triggerKey.Group);
                var job = RetrieveJob(new JobKey(trigger.JobKey.Name, trigger.JobKey.Group));
                var trigList = GetTriggersForJob(job.Key);

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
            var result = RemoveTrigger(triggerKey);
            StoreTrigger(newTrigger, true);
            return result;
        }

        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trig = session.Load<RavenTrigger>(triggerKey.Name + "/" + triggerKey.Group);

                return (trig == null) ? null : trig.Deserialize();
            }
        }

        public bool CalendarExists(string calName)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<RavenScheduler>(InstanceName);
                return sched.Calendars.ContainsKey(calName);
            }
        }

        public bool CheckExists(JobKey jobKey)
        {
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(jobKey.Name + "/" + jobKey.Group);
            return docMetaData != null;
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(triggerKey.Name + "/" + triggerKey.Group);
            return docMetaData != null;
        }

        public void ClearAllSchedulingData()
        {
            ClearAllSchedulingData<RavenScheduler>();
            ClearAllSchedulingData<RavenJob>();
            ClearAllSchedulingData<RavenTrigger>();
        }

        public void ClearAllSchedulingData<T>()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var objects = session.Query<T>().ToList();
                while (objects.Any())
                {
                    foreach (var obj in objects)
                    {
                        session.Delete(obj);
                    }

                    session.SaveChanges();
                    objects = session.Query<T>().ToList();
                }
            }
        }

        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            var calendarCopy = (ICalendar)calendar.Clone();

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<RavenScheduler>(InstanceName);

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
                    .Query<RavenTrigger>()
                    .Where(t => t.CalendarName == name)
                    .ToList();



                if (triggersToUpdate.Count == 0)
                {
                    session.SaveChanges();
                    return;
                }

                using (var bulkInsert = DocumentStoreHolder.Store.BulkInsert(options: new BulkInsertOptions() /*{ OverwriteExisting = true }*/))
                {
                    foreach (var t in triggersToUpdate)
                    {
                        var trigger = t.Deserialize();

                        trigger.UpdateWithNewCalendar(calendarCopy, misfireThreshold);
                        var updatedTrigger = new RavenTrigger(trigger)
                        {
                            State = t.State,
                            IsTimedTrigger = true

                        };
                        //overwrite
                        bulkInsert.Store(updatedTrigger, trigger.Key.Name + "/" + trigger.Key.Group);
                    }
                    session.SaveChanges();
                }
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
                var sched = session.Load<RavenScheduler>(InstanceName);
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

        public IDictionary<string, ICalendar> RetrieveCalendarCollection()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<RavenScheduler>(InstanceName);
                if (sched == null) throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                if (sched.Calendars == null) throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Calendar collection in '{0}' is null", InstanceName));
                return sched.Calendars;
            }
        }

        public int GetNumberOfJobs()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<RavenJob>().Count();
            }
        }

        public int GetNumberOfTriggers()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<RavenTrigger>().Count();
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
                HashSet<SimpleKey> queryResult;
                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    queryResult = session
                        .Query<RavenJob>()
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
                    .Query<RavenTrigger>()
                    .Where(t => op.Evaluate(t.TriggerKey.Group, compareToValue))
                    .Select(t => t.TriggerKey)
                    .ToHashSet();
            }
        }

        public IList<string> GetJobGroupNames()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<RavenJob>()
                    .Select(j => j.Key.Group)
                    .Distinct()
                    .ToList();
            }
        }

        public IList<string> GetTriggerGroupNames()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<RavenTrigger>()
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
                return session
                    .Query<RavenTrigger>()
                    .Where(t => Equals(t.JobKey, jobKey))
                    .Select(t => t.Deserialize())
                    .ToList();
            }
        }

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            RavenTrigger trigger;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                trigger = session.Load<RavenTrigger>(triggerKey.Name + "/" + triggerKey.Group);
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
                var trig = session.Load<RavenTrigger>(triggerKey.Name + "/" + triggerKey.Group);
                trig.State = (trig.State == InternalTriggerState.Blocked ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Paused);
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
                        .Query<RavenTrigger>()
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
            if (op.Equals(StringOperator.Equality))
            {
                IList<string> pausedGroupsSet;

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    pausedGroupsSet = session
                        .Query<RavenJob>()
                        .Where(j => j.Key.Group == compareToValue)
                        .Select(j => j.Key.Group)
                        .ToList();
                }

                foreach (string groupName in pausedGroupsSet)
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

                return pausedGroupsSet;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey)) return;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var ravenTrigger = session.Load<RavenTrigger>(triggerKey.Name + "/" + triggerKey.Group);

                // if the trigger is not paused resuming it does not make sense...
                if (ravenTrigger.State != InternalTriggerState.Paused &&
                    ravenTrigger.State != InternalTriggerState.PausedAndBlocked)
                {
                    return;
                }

                if (GetBlockedJobs().Contains(new SimpleKey(ravenTrigger.JobKey.Name, ravenTrigger.JobKey.Group)))
                {
                    ravenTrigger.State = InternalTriggerState.Blocked;
                }
                else
                {
                    ravenTrigger.State = InternalTriggerState.Waiting;
                }

                ApplyMisfire(ravenTrigger);

                if (ravenTrigger.State == InternalTriggerState.Waiting)
                {
                    ravenTrigger.IsTimedTrigger = true;
                }

                session.Store(ravenTrigger);
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
                var ravenTrigger = RetrieveTrigger(triggerKey);
     
                if (ravenTrigger != null)
                {
                    var jobGroup = ravenTrigger.JobKey.Group;
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
                    var sched = session.Load<RavenScheduler>(InstanceName);
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
                return session.Load<RavenScheduler>(InstanceName).PausedTriggerGroups;
            }
        }

        public Collection.ISet<string> GetPausedJobGroups()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<RavenScheduler>(InstanceName).PausedJobGroups;
            }
        }

        public Collection.ISet<SimpleKey> GetBlockedJobs()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<RavenScheduler>(InstanceName).BlockedJobs;
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
                var sched = session.Load<RavenScheduler>(InstanceName);

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
                var sched = session.Load<RavenScheduler>(InstanceName);

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
        /// <param name="ravenTrigger"></param>
        /// <returns></returns>
        protected virtual bool ApplyMisfire(RavenTrigger ravenTrigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = ravenTrigger.Deserialize().GetNextFireTimeUtc();
            if (!tnft.HasValue || tnft.Value > misfireTime
                || ravenTrigger.Deserialize().MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (ravenTrigger.Deserialize().CalendarName != null)
            {
                cal = RetrieveCalendar(ravenTrigger.Deserialize().CalendarName);
            }

            signaler.NotifyTriggerListenersMisfired((IOperableTrigger)ravenTrigger.Deserialize().Clone());

            ravenTrigger.Deserialize().UpdateAfterMisfire(cal);

            if (!ravenTrigger.Deserialize().GetNextFireTimeUtc().HasValue)
            {
                ravenTrigger.State = InternalTriggerState.Complete;
                signaler.NotifySchedulerListenersFinalized(ravenTrigger.Deserialize());
                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    ravenTrigger.IsTimedTrigger = true;
                    session.Store(ravenTrigger);
                    session.SaveChanges();
                }
            }
            else if (tnft.Equals(ravenTrigger.Deserialize().GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            List<IOperableTrigger> result = new List<IOperableTrigger>();
            Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
            Collection.ISet<RavenTrigger> excludedTriggers = new Collection.HashSet<RavenTrigger>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            // return empty list if store has no triggers.
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var timedTriggers = session
                    .Query<RavenTrigger>()
                    .Where(t => t.IsTimedTrigger)
                    .ToList();

                if (timedTriggers.Count == 0)
                {
                    return result;
                }


                while (true)
                {
                    var ravenTrigger = timedTriggers.First();
                    if (ravenTrigger == null)
                    {
                        break;
                    }

                    if (!timedTriggers.Remove(ravenTrigger))
                    {
                        break;
                    }

                    ravenTrigger.IsTimedTrigger = false;
                    session.Store(ravenTrigger);

                    if (ravenTrigger.Deserialize().GetNextFireTimeUtc() == null)
                    {
                        continue;
                    }

                    if (ApplyMisfire(ravenTrigger))
                    {
                        if (ravenTrigger.Deserialize().GetNextFireTimeUtc() != null)
                        {
                            ravenTrigger.IsTimedTrigger = true;
                            session.Store(ravenTrigger);
                        }
                        continue;
                    }

                    if (ravenTrigger.Deserialize().GetNextFireTimeUtc() > noLaterThan + timeWindow)
                    {
                        ravenTrigger.IsTimedTrigger = true;
                        session.Store(ravenTrigger);
                        break;
                    }

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // set it back as TimedTrigger and continue to search for next trigger.
                    JobKey jobKey = ravenTrigger.Deserialize().JobKey;
                    IJobDetail job = session
                        .Query<RavenJob>()
                        .First(j => Equals(j.Key, jobKey))
                        .Deserialize();

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            excludedTriggers.Add(ravenTrigger);
                            continue; // go to next trigger in store.
                        }
                        else
                        {
                            acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                        }
                    }

                    ravenTrigger.State = InternalTriggerState.Acquired;
                    ravenTrigger.Deserialize().FireInstanceId = GetFiredTriggerRecordId();
                    IOperableTrigger trig = (IOperableTrigger) ravenTrigger.Deserialize().Clone();
                    result.Add(trig);

                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = ravenTrigger.Deserialize().GetNextFireTimeUtc();
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }
                session.SaveChanges();
            }

            using (var bulkInsert = DocumentStoreHolder.Store.BulkInsert(options: new BulkInsertOptions() /*{ OverwriteExisting = true }*/))
            { 
                // If we did excluded triggers to prevent ACQUIRE State due to DisallowConcurrentExecution, we need to add them back to store.
                if (excludedTriggers.Count > 0)
                {
                    foreach (var trig in excludedTriggers)
                    {
                        trig.IsTimedTrigger = true;
                        bulkInsert.Store(trig);
                    }
                }
            }
            return result;

        }

        public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var ravenTrigger = session.Load<RavenTrigger>(trigger.Key.Name + "/" + trigger.Key.Group);
                if ((ravenTrigger == null) || (ravenTrigger.State != InternalTriggerState.Acquired))
                {
                    return;
                }
                ravenTrigger.State = InternalTriggerState.Waiting;
                ravenTrigger.IsTimedTrigger = true;
                session.Store(ravenTrigger);
                session.SaveChanges();
            }
        }
        

        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            List<TriggerFiredResult> results = new List<TriggerFiredResult>();
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                foreach (IOperableTrigger trigger in triggers)
                {
                    // was the trigger deleted since being acquired?
                    var ravenTrigger = session.Load<RavenTrigger>(trigger.Key.Name + "/" + trigger.Key.Group);
                    if (ravenTrigger?.Deserialize() == null)
                    {
                        continue;
                    }

                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    if (ravenTrigger.State != InternalTriggerState.Acquired)
                    {
                        continue;
                    }

                    ICalendar cal = null;
                    if (ravenTrigger.Deserialize().CalendarName != null)
                    {
                        cal = RetrieveCalendar(ravenTrigger.Deserialize().CalendarName);
                        if (cal == null)
                        {
                            continue;
                        }
                    }
                    DateTimeOffset? prevFireTime = trigger.GetPreviousFireTimeUtc();
                    // in case trigger was replaced between acquiring and firing
                    ravenTrigger.IsTimedTrigger = false;
                    // call triggered on our copy, and the scheduler's copy
                    ravenTrigger.Deserialize().Triggered(cal);
                    trigger.Triggered(cal);

                    ravenTrigger.State = InternalTriggerState.Waiting;

                    TriggerFiredBundle bndle = new TriggerFiredBundle(RetrieveJob(trigger.JobKey),
                        trigger,
                        cal,
                        false, SystemTime.UtcNow(),
                        trigger.GetPreviousFireTimeUtc(), prevFireTime,
                        trigger.GetNextFireTimeUtc());

                    IJobDetail job = bndle.JobDetail;

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        List<RavenTrigger> trigs = session.Query<RavenTrigger>()
                            .Where(t => Equals(t.JobKey, job.Key))
                            .ToList();

                        foreach (RavenTrigger tRavenTrigger in trigs)
                        {
                            if (tRavenTrigger.State == InternalTriggerState.Waiting)
                            {
                                tRavenTrigger.State = InternalTriggerState.Blocked;
                            }
                            if (tRavenTrigger.State == InternalTriggerState.Paused)
                            {
                                tRavenTrigger.State = InternalTriggerState.PausedAndBlocked;
                            }
                            tRavenTrigger.IsTimedTrigger = false;
                            session.Store(tRavenTrigger); // TODO Iftah - make sure it stores (and replaces) the original (tRavenTrigger is a loop variable)
                        }
                        var sched = session.Load<RavenScheduler>(job.Key.Name + "/" + job.Key.Group);
                        sched.BlockedJobs.Add(new SimpleKey(job.Key.Name, job.Key.Group));
                        session.Store(sched);
                    }
                    else if (ravenTrigger.Deserialize().GetNextFireTimeUtc() != null)
                    {
                        ravenTrigger.IsTimedTrigger = true;
                        session.Store(ravenTrigger);
                    }

                    results.Add(new TriggerFiredResult(bndle));
                }
                session.SaveChanges();
            }
            return results;
        }

        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var ravenTrigger = session.Load<RavenTrigger>(trigger.Key.Name + "/" + trigger.Key.Group);
                var sched = session.Load<RavenScheduler>(InstanceName);

                // It's possible that the job or trigger is null if it was deleted during execution
                var ravenJob = session.Load<RavenJob>(trigger.JobKey.Name + "/" + trigger.JobKey.Group);
                
                if (ravenJob != null)
                {
                    IJobDetail jd = ravenJob.Deserialize();

                    if (jobDetail.PersistJobDataAfterExecution)
                    {
                        JobDataMap newData = jobDetail.JobDataMap;
                        if (newData != null)
                        {
                            newData = (JobDataMap) newData.Clone();
                            newData.ClearDirtyFlag();
                        }
                        jd = jd.GetJobBuilder().SetJobData(newData).Build();
                        ravenJob = new RavenJob(jd);
                        session.Store(ravenJob);
                    }
                    if (jd.ConcurrentExecutionDisallowed)
                    {
                        sched.BlockedJobs.Remove(new SimpleKey(jd.Key.Name, jd.Key.Group));

                        List<RavenTrigger> trigs = session.Query<RavenTrigger>()
                            .Where(t => Equals(t.JobKey, jd.Key))
                            .ToList();

                        foreach (RavenTrigger tRavenTrigger in trigs)
                        {
                            if (tRavenTrigger.State == InternalTriggerState.Blocked)
                            {
                                tRavenTrigger.State = InternalTriggerState.Waiting;

                                tRavenTrigger.IsTimedTrigger = true;
                                session.Store(tRavenTrigger); // TODO Iftah - make sure it stores (and replaces) the original. (tRavenTrigger is a loop variable, might not work)
                            }
                            if (tRavenTrigger.State == InternalTriggerState.PausedAndBlocked)
                            {
                                tRavenTrigger.State = InternalTriggerState.Paused;
                            }
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
                if (ravenTrigger != null)
                {
                    if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                    {
                        // Deleting triggers
                        DateTimeOffset? d = trigger.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = ravenTrigger.Deserialize().GetNextFireTimeUtc();
                            if (!d.HasValue)
                            {
                                RemoveTrigger(trigger.Key);
                            }
                            else
                            {
                                //Deleting cancelled - trigger still active
                            }
                        }
                        else
                        {
                            RemoveTrigger(trigger.Key);
                            signaler.SignalSchedulingChange(null);
                        }
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                    {
                        ravenTrigger.State = InternalTriggerState.Complete;
                        ravenTrigger.IsTimedTrigger = false;
                        session.Store(ravenTrigger);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                    {
                        //Log.Info(string.Format(CultureInfo.InvariantCulture, "Trigger {0} set to ERROR State.", trigger.Key));
                        ravenTrigger.State = InternalTriggerState.Error;
                        session.Store(ravenTrigger);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                    {
                        //Log.Info(string.Format(CultureInfo.InvariantCulture, "All triggers of Job {0} set to ERROR State.", trigger.JobKey));
                        SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Error);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                    {
                        SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Complete);
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
                List<RavenTrigger> trigs = session.Query<RavenTrigger>()
                    .Where(t => Equals(t.JobKey, jobKey))
                    .ToList();

                foreach (var ravenTrigger in trigs)
                {
                    ravenTrigger.State = state;
                    if (state != InternalTriggerState.Waiting)
                    {
                        ravenTrigger.IsTimedTrigger = false;
                    }
                    session.Store(ravenTrigger);
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