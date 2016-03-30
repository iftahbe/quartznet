using System;
using System.Collections.Specialized;
using System.Threading;

using Quartz;
using Quartz.Impl;
using Quartz.Impl.Calendar;

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

            NameValueCollection properties = new NameValueCollection();

            properties["quartz.scheduler.instanceName"] = "TestScheduler";
            properties["quartz.scheduler.instanceId"] = "instance_one";
            properties["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz";
            properties["quartz.threadPool.threadCount"] = "1";
            properties["quartz.threadPool.threadPriority"] = "Normal";

            properties["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.JobStore, Quartz.Impl.RavenDB";
            properties["quartz.dataSource.default.connectionString"] = "Url=http://localhost:8080;DefaultDatabase=IftahDB";
            properties["quartz.dataSource.default.provider"] = "SqlServer-20";
            /*


              properties["quartz.jobStore.misfireThreshold"] = "60000";
              properties["quartz.jobStore.type"] = "Quartz.Impl.AdoJobStore.JobStoreTX, Quartz";
              properties["quartz.jobStore.useProperties"] = "false";
              properties["quartz.jobStore.dataSource"] = "default";
              properties["quartz.jobStore.tablePrefix"] = "QRTZ_";
              properties["quartz.jobStore.lockHandler.type"] = "Quartz.Impl.AdoJobStore.UpdateLockRowSemaphore, Quartz";
              properties["quartz.jobStore.driverDelegateType"] = "Quartz.Impl.AdoJobStore.SqlServerDelegate, Quartz";

              properties["quartz.dataSource.default.connectionString"] = "Server=DESKTOP-2AM9NOM\\SQLEXPRESS;Database=IftahDB;Trusted_Connection=True;";
              properties["quartz.dataSource.default.provider"] = "SqlServer-20";
     */

            try
            {
                /*

                 // create the thread pool 
                 SimpleThreadPool threadPool = new SimpleThreadPool(5, ThreadPriority.Normal); 
                 threadPool.Initialize(); 
                 // create the job store 
                 IJobStore jobStore = new JobStore(); 
                 
                 DirectSchedulerFactory.Instance.CreateScheduler("My Quartz Scheduler", "My Instance", threadPool, jobStore); 
                 // don't forget to start the scheduler: 
                 DirectSchedulerFactory.Instance.GetScheduler().Start();
                 */

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
                        .WithIntervalInSeconds(2)
                        .RepeatForever())
                    .Build();
                ITrigger trigger3 = TriggerBuilder.Create()
                    .WithIdentity("trigger3", "group1")
                    .WithDescription("Something")
                    .StartNow()
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(3)
                        .RepeatForever())
                    .Build();


                ITrigger trigger4 = TriggerBuilder.Create()
                    .WithIdentity("calendarTrigger", "group2")
                    .StartNow()
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(4)
                        .RepeatForever())
                    .ModifiedByCalendar("myHolidays") // but not on holidays
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

                scheduler.ScheduleJob(job1, triggerSet, true);
                scheduler.ScheduleJob(job2, trigger4);

                // some sleep to show what's happening
                Thread.Sleep(TimeSpan.FromSeconds(60));

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
            int count = (int?)context.MergedJobDataMap["Count"] ?? 1;

            Console.WriteLine("Greetings from ExampleJob1! Count:" + count);

            context.JobDetail.JobDataMap.Put("Count", ++count);
        }
    }

    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    public class ExampleJob2 : IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Greetings from ExampleJob 2!");
        }

    }

}
