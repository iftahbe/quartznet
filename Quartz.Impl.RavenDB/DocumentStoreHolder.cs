using System;
using Raven.Client;
using Raven.Client.Document;

namespace Quartz.Impl.RavenDB
{
    public class DocumentStoreHolder
    {
        private static readonly Lazy<IDocumentStore> store = new Lazy<IDocumentStore>(CreateStore);

        public static IDocumentStore Store => store.Value;

        private static IDocumentStore CreateStore()
        {
            var documentStore = new DocumentStore()
            {
                Url = RavenJobStore.Url,
                DefaultDatabase = RavenJobStore.DefaultDatabase
            };
            documentStore.RegisterListener(new ThreadPrinter());
            documentStore.Initialize();
            documentStore.Conventions.DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;
            return documentStore;
        }
    }
}
