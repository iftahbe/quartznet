using System;
using System.Collections.Generic;


namespace Quartz.Impl.RavenDB
{
    public class RavenScheduler
    {
        public string InstanceName { get; set; }
        public DateTimeOffset LastCheckinTime { get; set; }
        public DateTimeOffset CheckinInterval { get; set; }
        public string State { get; set; }
        public IDictionary<string, ICalendar> Calendars { get; set; }
        public Collection.ISet<string> Locks { get; set; }
        public Collection.ISet<string> PausedJobGroups { get; set; }
        public Collection.ISet<string> PausedTriggerGroups { get; set; }
        public Collection.ISet<SimpleKey> BlockedJobs { get; set; }
    }
}  