using System;
using System.Collections.Generic;


namespace Quartz.Impl.RavenDB
{
    public class Scheduler
    {
        public string InstanceName { get; set; }
        public DateTimeOffset LastCheckinTime { get; set; }
        public DateTimeOffset CheckinInterval { get; set; }
        public string State { get; set; }
        public Dictionary<string, ICalendar> Calendars { get; set; }
        public Collection.HashSet<string> Locks { get; set; }
        public Collection.HashSet<string> PausedJobGroups { get; set; }
        public Collection.HashSet<string> PausedTriggerGroups { get; set; }
        public Collection.HashSet<SimpleKey> BlockedJobs { get; set; }
    }
}  