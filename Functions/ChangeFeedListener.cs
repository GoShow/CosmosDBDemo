using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Functions;

public static class ChangeFeedListener
{
    [FunctionName("ChangeFeedListener")]
    public static void Run([CosmosDBTrigger(
            databaseName: "softuni",
            collectionName: "education",
            ConnectionStringSetting = "DocumentDBConnection",
            CreateLeaseCollectionIfNotExists = true,
            LeaseCollectionName = "leases")]IReadOnlyList<Document> documents,
        ILogger log)
    {
        //Track last change per document

        if (documents != null && documents.Count > 0)
        {
            foreach (var document in documents)
            {
                log.LogInformation(document.ToString());
            }
        }
    }
}

