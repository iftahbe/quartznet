using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Quartz.Impl.AdoJobStore;

namespace Quartz.Impl.RavenDB
{
    /// <summary>
    /// Helper class for returning the composite result of trying
    /// to recover misfired jobs.
    /// </summary>
    public class RecoverMisfiredJobsResult
    {
        public static readonly RecoverMisfiredJobsResult NoOp = new RecoverMisfiredJobsResult(false, 0, DateTimeOffset.MaxValue);

        /// <summary>
        /// Initializes a new instance of the <see cref="RecoverMisfiredJobsResult"/> class.
        /// </summary>
        /// <param name="hasMoreMisfiredTriggers">if set to <c>true</c> [has more misfired triggers].</param>
        /// <param name="processedMisfiredTriggerCount">The processed misfired trigger count.</param>
        /// <param name="earliestNewTimeUtc"></param>
        public RecoverMisfiredJobsResult(bool hasMoreMisfiredTriggers, int processedMisfiredTriggerCount, DateTimeOffset earliestNewTimeUtc)
        {
            this.HasMoreMisfiredTriggers = hasMoreMisfiredTriggers;
            this.ProcessedMisfiredTriggerCount = processedMisfiredTriggerCount;
            this.EarliestNewTime = earliestNewTimeUtc;
        }

        /// <summary>
        /// Gets a value indicating whether this instance has more misfired triggers.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance has more misfired triggers; otherwise, <c>false</c>.
        /// </value>
        public bool HasMoreMisfiredTriggers { get; }

        /// <summary>
        /// Gets the processed misfired trigger count.
        /// </summary>
        /// <value>The processed misfired trigger count.</value>
        public int ProcessedMisfiredTriggerCount { get; }

        public DateTimeOffset EarliestNewTime { get; }
    }
}
