using System;
using System.Collections.Specialized;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;

using Quartz;
using Quartz.Collection;
using Quartz.Impl;
using Quartz.Impl.Calendar;
using Quartz.Impl.RavenDB;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;

namespace Tryouts
{
    internal class Program
    {

        private static void Main(string[] args)
        {
            Common.Logging.LogManager.Adapter = new Common.Logging.Simple.ConsoleOutLoggerFactoryAdapter
            {
                Level = Common.Logging.LogLevel.Info
            };

            NameValueCollection properties = new NameValueCollection
            {
                ["quartz.scheduler.instanceName"] = "TestScheduler",
                ["quartz.scheduler.instanceId"] = "instance_one",
                ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
                ["quartz.threadPool.threadCount"] = "1",
                ["quartz.threadPool.threadPriority"] = "Normal",


   /*            
              
                ["quartz.jobStore.misfireThreshold"] = "60000",
                ["quartz.jobStore.type"] = "Quartz.Impl.AdoJobStore.JobStoreTX, Quartz",
                ["quartz.jobStore.useProperties"] = "false",
                ["quartz.jobStore.dataSource"] = "default",
                ["quartz.jobStore.tablePrefix"] = "QRTZ_",
                ["quartz.jobStore.lockHandler.type"] = "Quartz.Impl.AdoJobStore.UpdateLockRowSemaphore, Quartz",
                ["quartz.jobStore.driverDelegateType"] = "Quartz.Impl.AdoJobStore.SqlServerDelegate, Quartz",
                ["quartz.dataSource.default.connectionString"] = "Server=DESKTOP-2AM9NOM\\SQLEXPRESS;Database=IftahDB;Trusted_Connection=True;",
                ["quartz.dataSource.default.provider"] = "SqlServer-20"*/
                
            };
            
            try
            {
                // First we must get a reference to a scheduler
                ISchedulerFactory sf = new StdSchedulerFactory(properties);
                IScheduler scheduler = sf.GetScheduler();

                // and start it off
                scheduler.Start();
                
                // define the job and tie it to our ExampleJob class
                IJobDetail job1 = JobBuilder.Create<ExampleJob>()
                    .WithIdentity("job1", "group1")
                    .UsingJobData("myStringValue", "PI")
                    .UsingJobData("myFloatValue", 3.14f)
                    .Build();

                // define the job and tie it to our ExampleJob class
                IJobDetail job2 = JobBuilder.Create<ExampleJob2>()
                    .WithIdentity("job2", "group2")
                    .Build();


                // Trigger the job to run now, and then repeat every 10 seconds
                ITrigger trigger1 = TriggerBuilder.Create()
                    .WithIdentity("trigger1", "group1")
                    .StartNow()
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(1)
                        .RepeatForever())
                    .Build();

                ITrigger trigger2 = TriggerBuilder.Create()
                    .WithIdentity("trigger2", "group1")
                    .StartNow()
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(1)
                        .RepeatForever())
                    .Build();
                ITrigger trigger3 = TriggerBuilder.Create()
                    .WithIdentity("trigger3", "group1")
                    .WithDescription("Something")
                    .StartNow()
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(1)
                        .RepeatForever())
                    .Build();


                ITrigger trigger4 = TriggerBuilder.Create()
                    .WithIdentity("calendarTrigger", "group2")
                    .StartNow()
                    .WithCalendarIntervalSchedule()
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(3)
                        .RepeatForever())
                    //.ModifiedByCalendar("myHolidays") // but not on holidays
                    .Build();

                // Adding calendar for exluding days - triggers won't work on those days
                HolidayCalendar cal = new HolidayCalendar();
                cal.AddExcludedDate(new DateTime(2016, 3, 25));
                cal.AddExcludedDate(new DateTime(2016, 3, 24));
                scheduler.AddCalendar("myHolidays", cal, true, true);

                Quartz.Collection.ISet<ITrigger> triggerSet = new Quartz.Collection.HashSet<ITrigger>()
                {
                    trigger1,
                    trigger2,
                    trigger3
                };

                Quartz.Collection.ISet<ITrigger> triggerSet2 = new Quartz.Collection.HashSet<ITrigger>()
                {
                    trigger4
                };

                //scheduler.ScheduleJob(job1, trigger1);
                //scheduler.ScheduleJob(job1, trigger2);


                scheduler.ScheduleJob(job1, triggerSet, true);
                scheduler.ScheduleJob(job2, triggerSet2, true);

                // some sleep to show what's happening
                Thread.Sleep(TimeSpan.FromSeconds(600));

                // and last shut down the scheduler when you are ready to close your program
                scheduler.Shutdown();
            }
            catch (SchedulerException se)
            {
                Console.WriteLine(se);
            }

            Console.WriteLine("Press any key to close the application");
            Console.ReadKey();
        }
    }

    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    public class ExampleJob : IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            int count;
            count = context.MergedJobDataMap["Count"] == null ? 1 : context.MergedJobDataMap.GetIntValue("Count");

            Console.WriteLine("ExampleJob1 --> Trigger: {0} Count: {1}" ,context.Trigger.Key, count);

            context.JobDetail.JobDataMap.Put("Count", ++count);
                    }
    }

    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    public class ExampleJob2 : IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            Console.WriteLine("ExampleJob2 --> Trigger: {0}", context.Trigger.Key);
        }

    }

}
