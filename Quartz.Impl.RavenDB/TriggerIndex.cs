using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Quartz.Impl.RavenDB
{
    public class TriggerIndex : AbstractIndexCreationTask
    {
        public override string IndexName => "TriggerIndex";

        /// <summary>
        /// Creates the index definition.
        /// </summary>
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Map = @"from doc in docs.Triggers
                    select new {
	                    CalendarName = doc.CalendarName,
	                    JobGroup = doc.JobGroup,
	                    JobName = doc.JobName
                    }"
            };
        }
    }
}
