using Confluent.Kafka;
using Kafka_Producer_API.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Net;

namespace Kafka_Producer_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly string bootstrapServers = "localhost:9092";
        private readonly string topic = "kafka-test";

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] OrderRequest orderRequest)
        {
            string message = JsonConvert.SerializeObject(orderRequest);
            return Ok(await SendOrderRequest(this.topic, message));
        }

        private async Task<bool> SendOrderRequest(string topic,string message)
        {
            ProducerConfig config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = Dns.GetHostName()
            };

            try
            {
                using(var producer = new ProducerBuilder<string, string>(config).Build())
                {
                    Partition part = new Partition(3);
                    var topicPart = new TopicPartition(topic, part);
                    var result = await producer.ProduceAsync(topicPart, new Message<string, string>
                    {
                        Key="Deneme_key",
                        Value = message
                    });


                    Debug.WriteLine($"Delivery Timestamp : {result.Timestamp.UtcDateTime}");
                    if (result.Status ==PersistenceStatus.Persisted)
                    {
                        Debug.WriteLine($"Added to queue : {result.Timestamp.UtcDateTime} -- Topic Partit {result.TopicPartitionOffset}");
                    }
                    else if (result.Status == PersistenceStatus.NotPersisted)
                    {
                        Debug.WriteLine($"Not added to queue : {result.Timestamp.UtcDateTime}");
                    }
                    return await Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }

            return await Task.FromResult(false);
        }


    }
}
