using Confluent.Kafka;
using Kafka_Consumer_API.Models;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Text;

namespace Kafka_Consumer_API.Services
{
    public class ApacheKafkaConsumerService : IHostedService
    {
        private readonly string topic = "kafka-test";
        private readonly string groupId = "test_group";
        private readonly string bootstrapServers = "localhost:9092";
        private readonly HttpClient _httpClient;

        public ApacheKafkaConsumerService(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        static int i = 1;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                TopicPartition topPart = new TopicPartition(topic, new Partition(3));
                consumerBuilder.Subscribe(topic);
                consumerBuilder.Assign(topPart);
                var cancelToken = new CancellationTokenSource();

                try
                {
                    while (true)
                    {
                        var consumer = consumerBuilder.Consume(cancelToken.Token);
                        consumer.Partition = new Partition(3);

                        var orderConsumer = new OrderRequestConsumer()
                        {
                            OrderId = i,
                            CustomerId = i,
                            ProductId = i,
                            Quantity = i + 10,
                            Status = "denemeConsumer"
                        };

                        Debug.WriteLine($"Sent a new -- PT: {consumer.Partition.Value} --- OFFSET {consumer.Offset.Value} -- {consumer.Message.Value} ");

                        //var stringContent = new StringContent(JsonConvert.SerializeObject(orderConsumer), Encoding.UTF8, "application/json");
                        //var response = _httpClient.PostAsync("https://localhost:7253/api/producer", stringContent).Result;
                        //if (response.IsSuccessStatusCode)
                        //{
                        //    Debug.WriteLine($"Sent a new -- PT: {consumer.Partition.Value} --- OFFSET {consumer.Offset.Value} -- {consumer.Message.Value} ");
                        //    i++;
                        //}
                        //else
                        //{
                        //    Debug.WriteLine($"NOT sent a new request ... ");
                        //}
                        //var orderRequest = JsonConvert.DeserializeObject<OrderProcessingRequest>(consumer.Message.Value);
                        //Debug.WriteLine($"Processing Order Id : {orderRequest?.OrderId}");
                    }
                }
                catch (Exception ex)
                {
                    consumerBuilder.Close();
                    System.Diagnostics.Debug.WriteLine(ex.Message);
                }
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}



//try
//{
//    using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build())
//    {
//        TopicPartition topPart = new TopicPartition(topic, new Partition(3));
//        consumerBuilder.Subscribe(topic);
//        consumerBuilder.Assign(topPart);
//        var cancelToken = new CancellationTokenSource();

//        try
//        {
//            while (true)
//            {
//                var consumer = consumerBuilder.Consume(cancelToken.Token);
//                consumer.Partition = new Partition(3);

//                var orderConsumer = new OrderRequestConsumer()
//                {
//                    OrderId = i,
//                    CustomerId = i,
//                    ProductId = i,
//                    Quantity = i + 10,
//                    Status = "denemeConsumer"
//                };

//                var stringContent = new StringContent(JsonConvert.SerializeObject(orderConsumer), Encoding.UTF8, "application/json");
//                var response = _httpClient.PostAsync("https://localhost:7253/api/producer", stringContent).Result;
//                if (response.IsSuccessStatusCode)
//                {
//                    Debug.WriteLine($"Sent a new -- PT: {consumer.Partition.Value} --- OFFSET {consumer.Offset.Value} -- {consumer.Message.Value} ");
//                    i++;
//                }
//                else
//                {
//                    Debug.WriteLine($"NOT sent a new request ... ");
//                }
//                var orderRequest = JsonConvert.DeserializeObject<OrderProcessingRequest>(consumer.Message.Value);
//                Debug.WriteLine($"Processing Order Id : {orderRequest?.OrderId}");
//            }
//        }
//        catch (OperationCanceledException)
//        {
//            consumerBuilder.Close();
//        }
//    }
//}
//catch (Exception ex)
//{
//    System.Diagnostics.Debug.WriteLine(ex.Message);
//}