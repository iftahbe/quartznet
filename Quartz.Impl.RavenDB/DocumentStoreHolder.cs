using System;
using Raven.Client;
using Raven.Client.Document;

namespace Quartz.Impl.RavenDB
{
    public class DocumentStoreHolder
    {
        private readonly static Lazy<IDocumentStore> _store = new Lazy<IDocumentStore>(CreateStore);

        public static IDocumentStore Store
        {
            get { return _store.Value; }
        }

        private static IDocumentStore CreateStore()
        {
            var documentStore = new DocumentStore()
            {
                Url = RavenJobStore.Url,
                DefaultDatabase = RavenJobStore.DefaultDatabase
            }.Initialize();

            return documentStore;
        }
    }
}
