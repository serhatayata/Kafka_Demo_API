# Kafka_Demo_API
ASP.NET Core 6.0 - Kafka Integration

### Notes

#### These commands can change over time, when I searched some resources on the internet, they didn't work because of the version but for now these are working well.

Creating :

- kafka-topics.bat --create --topic test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

Reading :

- kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
