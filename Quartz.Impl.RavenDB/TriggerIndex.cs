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
	                    JobKey_Group = doc.JobKey.Group,
	                    JobKey_Name = doc.JobKey.Name,
	                    CalendarName = doc.CalendarName,
	                    JobKey = doc.JobKey
                    }"
            };
        }
    }
}
