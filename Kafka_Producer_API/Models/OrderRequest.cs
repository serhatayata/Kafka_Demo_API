namespace Kafka_Producer_API.Models
{
    public class OrderRequest
    {
        public int OrderId { get; set; }
        public int ProductId { get; set; }
        public int CustomerId { get; set; }
        public int Quantity { get; set; }
        public string Status { get; set; }
    }
}
