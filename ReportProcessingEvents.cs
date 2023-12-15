using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventGrid;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using System.Linq;
using FileTransferService.Core;

namespace FileTransferService.Functions.ReportProcessingEvents
{
    public class ReportProcessingEvents
    {
        private readonly IConfiguration _configuration;

        public ReportProcessingEvents(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [FunctionName("ReportProcessingEvents")]
        public async Task Run([EventGridTrigger]EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation(eventGridEvent.Data.ToString());

            CosmosClient cosmosClient = new CosmosClient(_configuration["EventDbUri"], _configuration["EventDbReadWriteKey"]);
            Database database = cosmosClient.GetDatabase(_configuration["EventDbName"]);
            Container container = database.GetContainer(_configuration["EventDbContainerName"]);

            TransferError transferErrorType;
            TransferInfo transferInfoType;
            ITransferBase transferInfo;

            if (eventGridEvent.EventType == "Error")
            {
                transferErrorType = eventGridEvent.Data.ToObjectFromJson<TransferError>();
                transferInfo = transferErrorType;
            }
            else
            {
                transferInfoType = eventGridEvent.Data.ToObjectFromJson<TransferInfo>();
                transferInfo = transferInfoType;
            }

            IOrderedQueryable<TransferEventsDocument> queryable = container.GetItemLinqQueryable<TransferEventsDocument>();
            var count = await queryable.Where(t => t.id == transferInfo.TransferId.Value.ToString()).CountAsync();

            log.LogInformation($"Count: {count}");
            if(count > 0)
            {
                log.LogInformation("Updating transferEventsDocument");
                FeedIterator<TransferEventsDocument> feed = queryable.Where(t => t.TransferId == transferInfo.TransferId).ToFeedIterator();
                FeedResponse<TransferEventsDocument> response = await feed.ReadNextAsync();

                TransferEventsDocument transferEventsDocument = response.First();
                transferEventsDocument.TransferEvents.Add(CreateTransferEvent());
                await container.ReplaceItemAsync(transferEventsDocument, transferEventsDocument.id, new PartitionKey(transferEventsDocument.OriginatingUserPrincipalName));
            }
            else
            {
                log.LogInformation("Creating transferEventsDocument");
                TransferEventsDocument transferEventsDocument = new TransferEventsDocument
                {
                    id = transferInfo.TransferId.Value.ToString(),
                    OriginatingUserPrincipalName = transferInfo.OriginatingUserPrincipalName,
                    OriginationDateTime = transferInfo.OriginationDateTime,
                    TransferId = transferInfo.TransferId
                };
                transferEventsDocument.TransferEvents.Add(CreateTransferEvent());
                try {
                    await container.CreateItemAsync(transferEventsDocument, new PartitionKey(transferEventsDocument.OriginatingUserPrincipalName));
                }
                catch(CosmosException ex)
                {
                    log.LogError(ex.Message);
                    if(ex.StatusCode == System.Net.HttpStatusCode.Conflict)
                    {
                        log.LogInformation("Conflict detected. Retrying...");
                        await Run(eventGridEvent, log);
                    }
                }
                               
            }

            TransferEvent CreateTransferEvent()
            {
                return new TransferEvent
                {
                    Id = Guid.NewGuid().ToString(),
                    Subject = eventGridEvent.Subject,
                    EventType = eventGridEvent.EventType,
                    EventTime = eventGridEvent.EventTime,
                    TransferInfo = transferInfo
                };
            }
            return;
        }
    }
}
