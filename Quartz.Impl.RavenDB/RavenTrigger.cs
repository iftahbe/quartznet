﻿using System;

using Quartz.Collection;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;

namespace Quartz.Impl.RavenDB
{
    public class RavenTrigger
    {
        public SimpleKey TriggerKey { get; set; }
        public SimpleKey JobKey { get; set; }
        public bool IsTimedTrigger { get; set; }
        public InternalTriggerState State { get; set; }

        public string Description { get; set; }
        public string CalendarName { get; set; }
        public JobDataMap JobDataMap { get; set; }
        public DateTimeOffset? FinalFireTimeUtc { get; set; }
        public int MisfireInstruction { get; set; }
        public DateTimeOffset? EndTimeUtc { get; set; }
        public DateTimeOffset StartTimeUtc { get; set; }
        public int Priority { get; set; }
        public bool HasMillisecondPrecision { get; set; }

        public CronOptions Cron { get; set; }
        public SimpleOptions Simp { get; set; }
        public CalendarOptions Cal { get; set; }
        public DailyTimeOptions Day { get; set; }

        public class CronOptions
        {
            public string CronExpression { get; set; }
            public TimeZoneInfo TimeZoneId { get; set; }
        }

        public class SimpleOptions
        {
            public int RepeatCount { get; set; }
            public TimeSpan RepeatInterval { get; set; }
        }

        public class CalendarOptions
        {

            public IntervalUnit RepeatIntervalUnit { get; set; }
            public int RepeatInterval { get; set; }
            public int TimesTriggered { get; set; }
            public TimeZoneInfo TimeZone { get; set; }
            public bool PreserveHourOfDayAcrossDaylightSavings { get; set; }
            public bool SkipDayIfHourDoesNotExist { get; set; }
        }

        public class DailyTimeOptions
        {
            public int RepeatCount { get; set; }
            public IntervalUnit RepeatIntervalUnit { get; set; }
            public int RepeatInterval { get; set; }        
            public TimeOfDay StartTimeOfDay { get; set; }
            public TimeOfDay EndTimeOfDay { get; set; }
            public ISet<DayOfWeek> DaysOfWeek { get; set; }
            public int TimesTriggered { get; set; }
            public TimeZoneInfo TimeZone { get; set; }

        }

        public RavenTrigger(IOperableTrigger newTrigger)
        {
            if (newTrigger == null) return;

            TriggerKey = new SimpleKey(newTrigger.Key.Name, newTrigger.Key.Group);
            JobKey = new SimpleKey(newTrigger.JobKey.Name, newTrigger.JobKey.Group);
            Description = newTrigger.Description;
            CalendarName = newTrigger.CalendarName;
            JobDataMap = newTrigger.JobDataMap;
            FinalFireTimeUtc = newTrigger.FinalFireTimeUtc;
            MisfireInstruction = newTrigger.MisfireInstruction;
            EndTimeUtc = newTrigger.EndTimeUtc;
            StartTimeUtc = newTrigger.StartTimeUtc;
            Priority = newTrigger.Priority;
            HasMillisecondPrecision = newTrigger.HasMillisecondPrecision;

            IsTimedTrigger = false;
            State = InternalTriggerState.Waiting;
            
            // Init trigger specific properties according to type of newTrigger. 
            // If an option doesn't apply to the type of trigger it will stay null by default.

            var cronTriggerImpl = newTrigger as CronTriggerImpl;
            if (cronTriggerImpl != null)
            {
                Cron = new CronOptions
                {
                    CronExpression = cronTriggerImpl.CronExpressionString,
                    TimeZoneId = cronTriggerImpl.TimeZone
                };
                return;
            }

            var simpTriggerImpl = newTrigger as SimpleTriggerImpl;
            if (simpTriggerImpl != null)
            {
                Simp = new SimpleOptions
                {
                    RepeatCount = simpTriggerImpl.RepeatCount,
                    RepeatInterval = simpTriggerImpl.RepeatInterval
                };
                return;
            }

            var calTriggerImpl = newTrigger as CalendarIntervalTriggerImpl;
            if (calTriggerImpl != null)
            {
                Cal = new CalendarOptions
                {
                    RepeatIntervalUnit = calTriggerImpl.RepeatIntervalUnit,
                    RepeatInterval = calTriggerImpl.RepeatInterval,
                    TimesTriggered = calTriggerImpl.TimesTriggered,
                    TimeZone = calTriggerImpl.TimeZone,
                    PreserveHourOfDayAcrossDaylightSavings = calTriggerImpl.PreserveHourOfDayAcrossDaylightSavings,
                    SkipDayIfHourDoesNotExist = calTriggerImpl.SkipDayIfHourDoesNotExist
                };
                return;
            }

            var dayTriggerImpl = newTrigger as DailyTimeIntervalTriggerImpl;
            if (dayTriggerImpl != null)
            {
                Day = new DailyTimeOptions
                {
                    RepeatCount = dayTriggerImpl.RepeatCount,
                    RepeatIntervalUnit = dayTriggerImpl.RepeatIntervalUnit,
                    RepeatInterval = dayTriggerImpl.RepeatInterval,
                    StartTimeOfDay = dayTriggerImpl.StartTimeOfDay,
                    EndTimeOfDay = dayTriggerImpl.EndTimeOfDay,
                    DaysOfWeek = dayTriggerImpl.DaysOfWeek,
                    TimesTriggered = dayTriggerImpl.TimesTriggered,
                    TimeZone = dayTriggerImpl.TimeZone
                };
            }
        }

        public IOperableTrigger Deserialize()
        {
            var triggerBuilder = TriggerBuilder.Create()
               .WithIdentity(TriggerKey.Name, TriggerKey.Group)
               .WithDescription(Description)
               .ModifiedByCalendar(CalendarName)
               .WithPriority(Priority)
               .StartAt(StartTimeUtc)
               .EndAt(EndTimeUtc)
               .ForJob(new JobKey(JobKey.Name, JobKey.Group))
               .UsingJobData(JobDataMap);

            if (Cron != null)
            {
                triggerBuilder = triggerBuilder.WithCronSchedule(Cron.CronExpression, builder =>
                {
                    builder
                        .InTimeZone(Cron.TimeZoneId);
                });
            }
            else if (Simp != null)
            {
                triggerBuilder = triggerBuilder.WithSimpleSchedule(builder =>
                {
                    builder
                        .WithInterval(Simp.RepeatInterval)
                        .WithRepeatCount(Simp.RepeatCount);
                });
            }
            else if (Cal != null)
            {
                triggerBuilder = triggerBuilder.WithCalendarIntervalSchedule(builder =>
                {
                    builder
                        .WithInterval(Cal.RepeatInterval, Cal.RepeatIntervalUnit)
                        .InTimeZone(Cal.TimeZone)
                        .PreserveHourOfDayAcrossDaylightSavings(Cal.PreserveHourOfDayAcrossDaylightSavings)
                        .SkipDayIfHourDoesNotExist(Cal.SkipDayIfHourDoesNotExist);
                });
            }
            else if (Day != null)
            {
                triggerBuilder = triggerBuilder.WithDailyTimeIntervalSchedule(builder =>
                {
                    builder
                        .WithRepeatCount(Day.RepeatCount)
                        .WithInterval(Day.RepeatInterval, Day.RepeatIntervalUnit)
                        .InTimeZone(Day.TimeZone)
                        .EndingDailyAt(Day.EndTimeOfDay)
                        .StartingDailyAt(Day.StartTimeOfDay)
                        .OnDaysOfTheWeek(Day.DaysOfWeek);
                });
            }

            var trigger = triggerBuilder.Build();
            return (IOperableTrigger) trigger;
        }

    }
}
